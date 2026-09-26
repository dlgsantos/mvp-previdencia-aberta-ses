# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver e Qualidade de Dados
# MAGIC
# MAGIC **Objetivo:** transformar os dados brutos em tabelas limpas, tipadas e integráveis, **diagnosticando cada problema antes de tratá-lo**.
# MAGIC
# MAGIC Princípios adotados:
# MAGIC 1. Nenhum valor é alterado silenciosamente. Cada tratamento segue o formato **PROBLEMA → EVIDÊNCIA → TRATAMENTO → JUSTIFICATIVA → TESTE**.
# MAGIC 2. Registros que não podem ser interpretados com segurança vão para `silver.quarentena`, sem serem descartados.
# MAGIC 3. Todos os testes são gravados em `silver.dq_resultados` (dimensão de qualidade, regra, métrica, valor, esperado, status, ação).
# MAGIC 4. Valores legítimos porém atípicos (ex.: estornos negativos) são **mantidos e sinalizados** com flags.
# MAGIC
# MAGIC | Tabela Silver | Origem Bronze | Grão |
# MAGIC |---|---|---|
# MAGIC | `empresa` | ses_cias | empresa |
# MAGIC | `empresa_grupo_mensal` | ses_grupos_economicos | empresa × mês |
# MAGIC | `contribuicoes` | ses_contrib_benef | empresa × mês × produto |
# MAGIC | `resgates` | ses_vgbl_resgates + ses_pgbl_resgates | empresa × mês × produto |
# MAGIC | `portabilidade` | ses_transferenciasexternas | empresa × mês × produto |
# MAGIC | `pmbac` | ses_vgbl_fundos + ses_pgbl_fundos | empresa × mês × produto |
# MAGIC | `dq_conciliacao_pgbl` | ses_contrib_benef, ses_pgbl_resgates × ses_pgbl_uf | empresa × mês × tipo de conciliação |
# MAGIC | `dq_resultados`, `quarentena` | todas | teste / registro |

# COMMAND ----------

# MAGIC %md ## 0. Configuração e funções de apoio

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F, Window

CATALOGO = "previdencia"
VOLUME_DIR = f"/Volumes/{CATALOGO}/bronze/raw/ses"
B = f"{CATALOGO}.bronze"
S = f"{CATALOGO}.silver"

PRODUTOS_ESCOPO = ["VGBL", "PGBL"]
RE_AAAAMM = r"^(19|20)\d{2}(0[1-9]|1[0-2])$"
RE_EMPRESA = r"^\d{5}$"

# Mapeamento da direção da portabilidade.
# Documentação SES: TIPOTRANSF = "Tipo de transferencia (Aceita ou Cedida)" — sem indicar as letras.
# Mapeamento CONFIRMADO na consulta oficial do SES online (Previdência: Portabilidades Externas), em 26/09/2026:
# empresa 04031, VGBL, 09/2025 -> 'Valor Aceito' R$ 2.757.725.564 / 12.805 = linha TIPOTRANSF 'R' do CSV;
#                                 'Valor Cedido' R$ 287.146.012 / 1.140  = linha TIPOTRANSF 'D' do CSV.
# Qualquer outro código (ex.: 'r', 'P') NÃO é interpretado: vai para quarentena.
MAPA_TIPOTRANSF = {"R": "ACEITA", "D": "CEDIDA"}

# Limiar para sinalizar quantidade de portabilidades implausível (valor médio por portabilidade abaixo de R$ 100)
LIMIAR_VALOR_MEDIO_PORTAB = 100

dq_linhas = []
def registra(tabela, dimensao, regra, metrica, valor, esperado, status, acao, camada="SILVER"):
    dq_linhas.append((camada, tabela, dimensao, regra, metrica, str(valor), str(esperado), status, acao))
    print(f"[{status}] {tabela} | {dimensao} | {regra} -> {metrica} = {valor} (esperado: {esperado})")

quarentenas = []
def envia_quarentena(df, tabela, motivo):
    n = df.count()
    if n > 0:
        quarentenas.append(df.select(
            F.lit(tabela).alias("tabela_origem"),
            F.lit(motivo).alias("motivo"),
            F.to_json(F.struct(*[F.col(f"`{c}`") for c in df.columns])).alias("registro_json")))
    return n

def dec(c):
    """Texto com vírgula decimal -> DECIMAL(20,2). try_cast: valores não numéricos viram NULL e são contados antes."""
    return F.expr(f"try_cast(regexp_replace(trim(`{c}`), ',', '.') AS DECIMAL(20,2))")

def mes_ref(c):
    """AAAAMM -> DATE (1º dia do mês). Só converte se o texto for um AAAAMM válido."""
    return F.when(F.trim(F.col(c)).rlike(RE_AAAAMM), F.to_date(F.concat(F.trim(F.col(c)), F.lit("01")), "yyyyMMdd"))

def conta_nao_numericos(df, cols):
    exprs = [F.sum(F.when(F.col(c).isNotNull() & (F.trim(F.col(c)) != "") & dec(c).isNull(), 1).otherwise(0)).alias(c) for c in cols]
    return df.agg(*exprs).first().asDict()

def conta_com_espaco(df, cols):
    exprs = [F.sum(F.when(F.col(c) != F.trim(F.col(c)), 1).otherwise(0)).alias(c) for c in cols]
    return df.agg(*exprs).first().asDict()

def teste_unicidade(df, chave, tabela):
    total = df.count()
    distintas = df.select(*chave).distinct().count()
    dup = total - distintas
    registra(tabela, "Unicidade", f"chave única {chave}", "nº de chaves duplicadas", dup, 0,
             "OK" if dup == 0 else "FALHA", "falhar o pipeline / investigar")
    if dup > 0:
        raise ValueError(f"Chave duplicada em {tabela}: {dup}")

COLS_META = ["_arquivo_origem", "_data_geracao_base_ses", "_mes_referencia_max", "_ts_ingestao", "_hash_linha"]
def negocio(df):
    return [c for c in df.columns if c not in COLS_META]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Diagnóstico de formato dos arquivos
# MAGIC
# MAGIC ### 1.1 Encoding Windows-1252
# MAGIC **Problema:** os CSVs do SES são gravados em Windows-1252. Lidos como UTF-8 (padrão do Spark), os acentos são corrompidos.
# MAGIC **Evidência:** abaixo, o mesmo arquivo lido nos dois charsets.
# MAGIC **Tratamento:** leitura com `encoding=windows-1252` (já aplicada na Bronze). **Teste:** nenhum caractere de substituição (`�`) nos nomes.

# COMMAND ----------

caminho_cias = [f.path for f in dbutils.fs.ls(VOLUME_DIR) if f.name.lower() == "ses_cias.csv"][0]
leitura = lambda enc: spark.read.option("header", "true").option("sep", ";").option("encoding", enc).csv(caminho_cias)
df_utf8, df_1252 = leitura("UTF-8"), leitura("windows-1252")

n_corrompidos_utf8 = df_utf8.filter(F.col("Noenti").contains("�")).count()
n_corrompidos_1252 = df_1252.filter(F.col("Noenti").contains("�")).count()
exemplos = (df_utf8.select(F.trim("Coenti").alias("cod"), F.col("Noenti").alias("lido_como_utf8"))
            .join(df_1252.select(F.trim("Coenti").alias("cod"), F.col("Noenti").alias("lido_como_windows_1252")), "cod")
            .filter(F.col("lido_como_utf8").contains("�")).limit(5))
display(exemplos)

registra("bronze.ses_cias", "Consistência", "encoding correto (sem caractere �)", "nomes corrompidos lendo como UTF-8", n_corrompidos_utf8, ">0 (demonstração)", "INFO", "ler como windows-1252")
registra("bronze.ses_cias", "Consistência", "encoding correto (sem caractere �)", "nomes corrompidos lendo como windows-1252", n_corrompidos_1252, 0,
         "OK" if n_corrompidos_1252 == 0 else "FALHA", "revisar charset")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Separador `;`, vírgula decimal e período AAAAMM
# MAGIC **Problema:** números usam vírgula como separador decimal (`350407,58`) e alguns carregam resíduo de ponto flutuante (`31557844,9700002`); o período vem como texto `AAAAMM`.
# MAGIC **Tratamento:** `regexp_replace(',', '.')` + `try_cast` para `DECIMAL(20,2)`; `AAAAMM` → `DATE` (1º dia do mês) apenas quando o texto é um mês válido.
# MAGIC **Teste:** nº de valores não vazios que não viram número = 0; nº de meses inválidos.

# COMMAND ----------

amostra = spark.table(f"{B}.ses_contrib_benef")
pct_virgula = amostra.select(F.avg(F.col("contrib").contains(",").cast("int")).alias("p")).first()["p"]
registra("bronze.ses_contrib_benef", "Consistência", "formato numérico", "% de valores de contrib com vírgula decimal", round(pct_virgula * 100, 1), ">0 (formato BR)", "INFO", "converter vírgula -> ponto e tipar DECIMAL(20,2)")
display(amostra.select("coenti", "damesano", "tipoProd", "contrib", dec("contrib").alias("contrib_convertido")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Nomes de colunas inconsistentes
# MAGIC **Problema:** a mesma chave aparece como `coenti`, `COENTI` e `Coenti`; o mês como `damesano` e `DAMESANO`. **Tratamento:** nomes padronizados em *snake_case* (`cod_empresa`, `ano_mes`, `mes_ref`...).

# COMMAND ----------

TABELAS_BRONZE = ["ses_contrib_benef", "ses_vgbl_resgates", "ses_pgbl_resgates", "ses_transferenciasexternas",
                  "ses_vgbl_fundos", "ses_pgbl_fundos", "ses_pgbl_uf", "ses_cias", "ses_grupos_economicos"]
PADRAO = {"coenti": "cod_empresa", "damesano": "ano_mes"}
nomes = []
for t in TABELAS_BRONZE:
    for c in negocio(spark.table(f"{B}.{t}")):
        if c.lower() in PADRAO:
            nomes.append((t, c, PADRAO[c.lower()]))
display(spark.createDataFrame(nomes, "tabela_bronze string, nome_original string, nome_padronizado string").orderBy("nome_padronizado", "nome_original"))
variantes = len({n[1] for n in nomes if n[2] == "cod_empresa"})
registra("bronze.*", "Consistência", "nome único por conceito", "variantes do nome da coluna de empresa", variantes, 1, "ALERTA" if variantes > 1 else "OK", "padronizar para cod_empresa")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Espaços extras nas chaves
# MAGIC **Problema:** em alguns arquivos `coenti` e `damesano` vêm com espaços à direita (`03166     `). Sem tratamento, o *join* entre tabelas falha silenciosamente.
# MAGIC **Tratamento:** `trim` em todas as colunas de texto. **Teste:** após o trim, 100% dos códigos de empresa com 5 dígitos.

# COMMAND ----------

espacos = []
for t in TABELAS_BRONZE:
    df = spark.table(f"{B}.{t}")
    chaves = [c for c in negocio(df) if c.lower() in PADRAO]
    for c, n in conta_com_espaco(df, chaves).items():
        espacos.append((t, c, n, df.count()))
        if n > 0:
            registra(f"bronze.{t}", "Consistência", "chave sem espaços", f"linhas com espaço em {c}", n, 0, "ALERTA", "aplicar trim")
display(spark.createDataFrame(espacos, "tabela string, coluna string, linhas_com_espaco long, total_linhas long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. `silver.empresa` (cadastro)
# MAGIC **Problemas:** chaves com espaço; `Cogrupo` e `Nogrupo` **100% vazios** (a própria documentação diz "informação ainda não disponível").
# MAGIC **Tratamento:** trim; exclusão das colunas vazias (o grupo virá de `ses_grupos_economicos`, com dimensão temporal).
# MAGIC **Testes:** completude das colunas excluídas (evidência), unicidade e formato do código.

# COMMAND ----------

bz = spark.table(f"{B}.ses_cias")
total = bz.count()
for c in ["Cogrupo", "Nogrupo"]:
    nulos = bz.filter(F.col(c).isNull() | (F.trim(F.col(c)) == "")).count()
    registra("bronze.ses_cias", "Completude", f"{c} preenchido", "% nulo/vazio", round(100 * nulos / total, 1), "—", "ALERTA" if nulos == total else "OK",
             "coluna 100% vazia excluída; grupo obtido de ses_grupos_economicos")

empresa = bz.select(F.trim("Coenti").alias("cod_empresa"), F.trim("Noenti").alias("nome_empresa"))
fora_padrao = empresa.filter(~F.col("cod_empresa").rlike(RE_EMPRESA)).count()
registra("silver.empresa", "Consistência", "cod_empresa com 5 dígitos", "nº fora do padrão", fora_padrao, 0, "OK" if fora_padrao == 0 else "FALHA", "quarentena")
teste_unicidade(empresa, ["cod_empresa"], "silver.empresa")
empresa.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.empresa")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. `silver.empresa_grupo_mensal` (grupo econômico no tempo)
# MAGIC **Problemas:** o grupo de uma empresa **muda ao longo do tempo** (a tabela é um retrato mensal); há linhas com mês vazio, grupo nulo e duplicatas exatas.
# MAGIC **Tratamento:** trim; mês inválido e grupo nulo → quarentena; duplicatas exatas removidas; teste de 1 grupo por empresa×mês.
# MAGIC **Justificativa:** a Gold atribuirá a cada fluxo o grupo **vigente no mês** do fluxo.

# COMMAND ----------

bz = spark.table(f"{B}.ses_grupos_economicos")
g = bz.select(F.trim("coenti").alias("cod_empresa"), F.trim("damesano").alias("ano_mes"), mes_ref("damesano").alias("mes_ref"),
              F.trim("noenti").alias("nome_empresa_no_mes"), F.trim("cogrupo").alias("cod_grupo"), F.trim("nogrupo").alias("nome_grupo"))

invalidos = g.filter(F.col("mes_ref").isNull())
n_inv = envia_quarentena(invalidos, "ses_grupos_economicos", "mês (damesano) vazio ou inválido")
registra("silver.empresa_grupo_mensal", "Consistência", "AAAAMM válido", "linhas com mês inválido", n_inv, 0, "OK" if n_inv == 0 else "ALERTA", "quarentena")

sem_grupo = g.filter(F.col("mes_ref").isNotNull() & (F.col("cod_grupo").isNull() | (F.col("cod_grupo") == "")))
n_sg = envia_quarentena(sem_grupo, "ses_grupos_economicos", "código de grupo vazio")
registra("silver.empresa_grupo_mensal", "Completude", "cod_grupo preenchido", "linhas sem grupo", n_sg, 0, "OK" if n_sg == 0 else "ALERTA", "quarentena")

g = g.filter(F.col("mes_ref").isNotNull() & F.col("cod_grupo").isNotNull() & (F.col("cod_grupo") != ""))
antes = g.count(); g = g.dropDuplicates(); dup_exatas = antes - g.count()
registra("silver.empresa_grupo_mensal", "Unicidade", "sem duplicatas exatas", "linhas duplicadas removidas", dup_exatas, 0, "OK" if dup_exatas == 0 else "ALERTA", "remover duplicata exata")

g = g.withColumn("fl_grupo_generico", F.col("cod_grupo") == "99999")
teste_unicidade(g, ["cod_empresa", "mes_ref"], "silver.empresa_grupo_mensal")

mudaram = g.groupBy("cod_empresa").agg(F.countDistinct("cod_grupo").alias("n")).filter("n > 1").count()
registra("silver.empresa_grupo_mensal", "Consistência (temporal)", "grupo muda no tempo?", "empresas com mais de um grupo no histórico", mudaram, "—", "INFO",
         "atribuição de grupo por empresa+mês (não pelo grupo atual)")
g.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.empresa_grupo_mensal")
display(g.groupBy("cod_empresa").agg(F.countDistinct("cod_grupo").alias("n_grupos"), F.collect_set("nome_grupo").alias("grupos"))
        .filter("n_grupos > 1").orderBy(F.desc("n_grupos")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. `silver.contribuicoes`
# MAGIC **Problemas:** vírgula decimal; `tipoProd` inclui `PrevTrad` (fora do escopo); contribuições **negativas** existem (estornos/ajustes informados pela empresa).
# MAGIC **Tratamento:** tipagem; filtro de escopo VGBL/PGBL **com contagem do que foi excluído**; negativos **mantidos** com `fl_contribuicao_negativa`.

# COMMAND ----------

bz = spark.table(f"{B}.ses_contrib_benef")
dominio = [r["tipoProd"] for r in bz.select("tipoProd").distinct().collect()]
fora_dom = sorted(set(dominio) - {"PGBL", "VGBL", "PrevTrad"})
registra("silver.contribuicoes", "Consistência (domínio)", "tipoProd ∈ {PGBL, VGBL, PrevTrad}", "valores fora do domínio", fora_dom or 0, 0, "OK" if not fora_dom else "FALHA", "investigar")
nn = conta_nao_numericos(bz, ["contrib", "benef"])
registra("silver.contribuicoes", "Validade", "valores numéricos", "não numéricos (contrib, benef)", nn, "{0, 0}", "OK" if sum(nn.values()) == 0 else "FALHA", "quarentena")

excl = bz.filter(~F.col("tipoProd").isin(PRODUTOS_ESCOPO))
registra("silver.contribuicoes", "Escopo", "somente VGBL e PGBL", "linhas PrevTrad excluídas (decisão de escopo)", excl.count(), "—", "INFO", "fora do escopo do MVP")

contrib = (bz.filter(F.col("tipoProd").isin(PRODUTOS_ESCOPO))
           .select(F.trim("coenti").alias("cod_empresa"), F.trim("damesano").alias("ano_mes"), mes_ref("damesano").alias("mes_ref"),
                   F.col("tipoProd").alias("cod_produto"), dec("contrib").alias("vl_contribuicao"), dec("benef").alias("vl_beneficio"))
           .withColumn("fl_contribuicao_negativa", F.col("vl_contribuicao") < 0))
inv = contrib.filter(F.col("mes_ref").isNull()).count()
registra("silver.contribuicoes", "Consistência", "AAAAMM válido", "linhas com mês inválido", inv, 0, "OK" if inv == 0 else "FALHA", "quarentena")
neg = contrib.filter("fl_contribuicao_negativa").agg(F.count("*").alias("n"), F.sum("vl_contribuicao").alias("soma")).first()
registra("silver.contribuicoes", "Validade", "contribuição >= 0", "linhas negativas / soma (R$)", f"{neg['n']} / {neg['soma']}", "0 (regra de negócio)", "ALERTA",
         "manter (estornos legítimos no FIP) e sinalizar com fl_contribuicao_negativa")
teste_unicidade(contrib, ["cod_empresa", "mes_ref", "cod_produto"], "silver.contribuicoes")
contrib.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.contribuicoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. `silver.resgates` — duplicatas exatas e **mudança de granularidade**
# MAGIC
# MAGIC **Problema 1 — coluna não documentada:** os arquivos têm `Resg_Pag_programado`, ausente da documentação oficial (que lista só `resg_total` e `resg_parcial`).
# MAGIC **Problema 2 — duplicatas exatas:** linhas idênticas repetidas.
# MAGIC **Problema 3 — granularidade:** até 11/2013 há **1 linha por empresa×mês**; a partir de 12/2013 aparecem várias linhas por empresa×mês **sem nenhuma coluna que as diferencie**.
# MAGIC
# MAGIC **Tratamento:**
# MAGIC 1. Unir VGBL e PGBL em uma só tabela com a coluna `cod_produto`.
# MAGIC 2. Remover duplicatas exatas **somente após provar que a soma removida é zero** (se não for, o pipeline para).
# MAGIC 3. Somar as sub-linhas por empresa×mês×produto (**não** é duplicidade: são parcelas do mesmo total).
# MAGIC 4. `vl_resgate = resg_total + resg_parcial` (definição do projeto); `Resg_Pag_programado` mantido em coluna própria, fora da soma.
# MAGIC
# MAGIC **Testes:** soma removida = 0; chave única após agregação; continuidade da série mensal na quebra de 12/2013.

# COMMAND ----------

def le_resgates(tabela, produto):
    df = spark.table(f"{B}.{tabela}")
    cols = [c.lower() for c in negocio(df)]
    registra(f"bronze.{tabela}", "Consistência (schema × documentação)", "colunas documentadas: damesano, coenti, resg_total, resg_parcial",
             "colunas não documentadas", [c for c in cols if c not in ("damesano", "coenti", "resg_total", "resg_parcial")], "[]", "ALERTA",
             "manter resg_pag_programado em coluna própria e registrar no catálogo")
    return df.select(F.lit(produto).alias("cod_produto"), F.trim("coenti").alias("cod_empresa"), F.trim("damesano").alias("ano_mes"),
                     F.trim("resg_total").alias("resg_total"), F.trim("resg_parcial").alias("resg_parcial"),
                     F.trim("Resg_Pag_programado").alias("resg_pag_programado"))

res_txt = le_resgates("ses_vgbl_resgates", "VGBL").unionByName(le_resgates("ses_pgbl_resgates", "PGBL"))
nn = conta_nao_numericos(res_txt, ["resg_total", "resg_parcial", "resg_pag_programado"])
registra("silver.resgates", "Validade", "valores numéricos", "não numéricos", nn, "{0,0,0}", "OK" if sum(nn.values()) == 0 else "FALHA", "quarentena")

valor = lambda df: df.select((F.coalesce(dec("resg_total"), F.lit(0)) + F.coalesce(dec("resg_parcial"), F.lit(0)) + F.coalesce(dec("resg_pag_programado"), F.lit(0))).alias("v"))

# --- duplicatas exatas
linhas_antes = res_txt.count()
res_dedup = res_txt.dropDuplicates()
linhas_dup = linhas_antes - res_dedup.count()
soma_antes = valor(res_txt).agg(F.sum("v")).first()[0]
soma_depois = valor(res_dedup).agg(F.sum("v")).first()[0]
soma_removida = soma_antes - soma_depois
registra("silver.resgates", "Unicidade", "duplicatas exatas", "linhas duplicadas removidas", linhas_dup, "—", "ALERTA", "remover somente se a soma removida for zero")
registra("silver.resgates", "Acurácia", "remoção de duplicatas não altera totais", "soma removida (R$)", soma_removida, 0, "OK" if soma_removida == 0 else "FALHA", "abortar e investigar")
if soma_removida != 0:
    raise ValueError("Duplicatas exatas carregam valor: não é seguro removê-las.")

# COMMAND ----------

# MAGIC %md #### Evidência da mudança de granularidade (linhas por empresa×mês, por ano)

# COMMAND ----------

por_chave = (res_dedup.groupBy("cod_produto", "cod_empresa", "ano_mes").count()
             .groupBy("cod_produto", F.substring("ano_mes", 1, 4).alias("ano"))
             .agg(F.round(F.avg("count"), 2).alias("media_linhas_por_empresa_mes"), F.max("count").alias("max_linhas_por_empresa_mes")))
display(por_chave.orderBy("cod_produto", "ano"))
primeiro = res_dedup.groupBy("cod_produto", "cod_empresa", "ano_mes").count().filter("count > 1").agg(F.min("ano_mes")).first()[0]
registra("silver.resgates", "Consistência (temporal)", "grão constante ao longo do tempo", "primeiro mês com >1 linha por empresa×mês (após remover duplicatas)", primeiro, "—", "ALERTA",
         "agregar por soma para empresa×mês×produto")

# COMMAND ----------

resgates = (res_dedup
            .groupBy("cod_empresa", "ano_mes", "cod_produto")
            .agg(F.sum(dec("resg_total")).alias("vl_resgate_total"),
                 F.sum(dec("resg_parcial")).alias("vl_resgate_parcial"),
                 F.sum(dec("resg_pag_programado")).alias("vl_resgate_pag_programado"),
                 F.count("*").alias("qt_linhas_origem"))
            .withColumn("mes_ref", mes_ref("ano_mes"))
            .withColumn("vl_resgate", F.coalesce("vl_resgate_total", F.lit(0)) + F.coalesce("vl_resgate_parcial", F.lit(0)))
            .select("cod_empresa", "mes_ref", "ano_mes", "cod_produto", "vl_resgate_total", "vl_resgate_parcial", "vl_resgate",
                    "vl_resgate_pag_programado", "qt_linhas_origem"))
inv = resgates.filter(F.col("mes_ref").isNull()).count()
registra("silver.resgates", "Consistência", "AAAAMM válido", "linhas com mês inválido", inv, 0, "OK" if inv == 0 else "FALHA", "quarentena")
neg = resgates.filter("vl_resgate < 0").count()
registra("silver.resgates", "Validade", "resgate >= 0", "empresa×mês com resgate negativo", neg, 0, "OK" if neg == 0 else "ALERTA", "manter e sinalizar")
teste_unicidade(resgates, ["cod_empresa", "mes_ref", "cod_produto"], "silver.resgates")
resgates.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.resgates")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Teste de continuidade na quebra de granularidade
# MAGIC Se a agregação estiver correta, a passagem de 11/2013 para 12/2013 não deve produzir um salto fora do padrão histórico.
# MAGIC Métrica: variação % do total mensal em 12/2013 comparada ao percentil 90 das variações mensais absolutas de 2010–2016.

# COMMAND ----------

serie = (spark.table(f"{S}.resgates").groupBy("cod_produto", "mes_ref").agg(F.sum("vl_resgate").alias("total"))
         .withColumn("var_pct", (F.col("total") / F.lag("total").over(Window.partitionBy("cod_produto").orderBy("mes_ref")) - 1).cast("double")))
display(serie.filter("mes_ref BETWEEN '2013-06-01' AND '2014-06-01'").orderBy("cod_produto", "mes_ref"))
for p in PRODUTOS_ESCOPO:
    sp = serie.filter(F.col("cod_produto") == p)
    p90 = sp.filter("mes_ref BETWEEN '2010-01-01' AND '2016-12-01'").select(F.percentile_approx(F.abs("var_pct"), 0.9)).first()[0]
    v = sp.filter("mes_ref = '2013-12-01'").select("var_pct").first()[0]
    registra("silver.resgates", "Consistência (temporal)", f"continuidade {p} em 12/2013", "|variação % em 12/2013| vs p90 histórico",
             f"{abs(v)*100:.1f}% vs {p90*100:.1f}%", "<= p90", "OK" if abs(v) <= p90 else "ALERTA", "se ALERTA, investigar antes da Gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. `silver.portabilidade` — domínio de `TIPOTRANSF`
# MAGIC **Problemas:**
# MAGIC - `TIPOTRANSF` deveria ter 2 valores (aceita/cedida), mas aparecem `D`, `R`, `r` e `P`;
# MAGIC - `TIPOPLANO` tem valores vazios e modalidades além de VGBL/PGBL;
# MAGIC - `QUANTIDADE` tem valores implausíveis (ex.: 2,75 bilhões de portabilidades em um mês).
# MAGIC
# MAGIC **Tratamento:**
# MAGIC - `R`→ACEITA, `D`→CEDIDA (parâmetro `MAPA_TIPOTRANSF`), **mapeamento confirmado na consulta oficial do SES online** (empresa 04031, VGBL, 09/2025: aceito R$ 2.757.725.564 = linha `R`; cedido R$ 287.146.012 = linha `D`, com as mesmas quantidades 12.805 e 1.140); **`r` e `P` não são interpretados** → quarentena;
# MAGIC - apenas `TIPOPLANO` ∈ {VGBL, PGBL} entra no escopo; o restante é quantificado;
# MAGIC - `QUANTIDADE` não é usada nas análises; linhas com valor médio < R$ 100 por portabilidade recebem `fl_qtd_portab_suspeita`.

# COMMAND ----------

bz = spark.table(f"{B}.ses_transferenciasexternas")
display(bz.groupBy("TIPOTRANSF").agg(F.count("*").alias("linhas"), F.sum(dec("VALOR")).alias("valor_total")).orderBy("TIPOTRANSF"))
display(bz.groupBy("TIPOPLANO").agg(F.count("*").alias("linhas"), F.sum(dec("VALOR")).alias("valor_total")).orderBy(F.desc("valor_total")))

pt = bz.select(F.trim("COENTI").alias("cod_empresa"), F.trim("DAMESANO").alias("ano_mes"), mes_ref("DAMESANO").alias("mes_ref"),
               F.trim("TIPOTRANSF").alias("tipotransf"), F.trim("TIPOPLANO").alias("tipoplano"),
               dec("VALOR").alias("vl_portabilidade"), F.expr("try_cast(trim(QUANTIDADE) AS BIGINT)").alias("qt_portabilidades"))
nn = conta_nao_numericos(bz, ["VALOR"])
registra("silver.portabilidade", "Validade", "VALOR numérico", "não numéricos", nn["VALOR"], 0, "OK" if nn["VALOR"] == 0 else "FALHA", "quarentena")
teste_unicidade(pt, ["cod_empresa", "mes_ref", "tipotransf", "tipoplano"], "silver.portabilidade (origem)")

fora = pt.filter(~F.col("tipotransf").isin(list(MAPA_TIPOTRANSF.keys())))
vf = fora.agg(F.sum("vl_portabilidade")).first()[0]
n_fora = envia_quarentena(fora, "ses_transferenciasexternas", "TIPOTRANSF fora do domínio documentado (não interpretado)")
registra("silver.portabilidade", "Consistência (domínio)", f"TIPOTRANSF ∈ {list(MAPA_TIPOTRANSF.keys())}", "linhas fora do domínio / valor (R$)", f"{n_fora} / {vf}", 0, "ALERTA", "quarentena, sem interpretação")

pt = pt.filter(F.col("tipotransf").isin(list(MAPA_TIPOTRANSF.keys())))
vazio = pt.filter(F.col("tipoplano").isNull() | (F.col("tipoplano") == ""))
registra("silver.portabilidade", "Completude", "TIPOPLANO preenchido", "linhas sem produto / valor (R$)",
         f"{vazio.count()} / {vazio.agg(F.sum('vl_portabilidade')).first()[0]}", 0, "ALERTA", "fora do escopo (produto não identificado)")
tot = pt.agg(F.sum("vl_portabilidade")).first()[0]
esc = pt.filter(F.col("tipoplano").isin(PRODUTOS_ESCOPO)).agg(F.sum("vl_portabilidade")).first()[0]
registra("silver.portabilidade", "Escopo", "TIPOPLANO ∈ {VGBL, PGBL}", "% do valor de portabilidade dentro do escopo", round(100 * float(esc) / float(tot), 2), "—", "INFO",
         "demais modalidades (PAGP, PRGP, VAGP, VRGP, Previdência, vazio...) fora do escopo")

pt = (pt.filter(F.col("tipoplano").isin(PRODUTOS_ESCOPO))
        .withColumn("tipo_transferencia", F.create_map(*[F.lit(x) for kv in MAPA_TIPOTRANSF.items() for x in kv])[F.col("tipotransf")])
        .withColumn("fl_qtd_portab_suspeita", (F.col("qt_portabilidades") > 0) & (F.col("vl_portabilidade") / F.col("qt_portabilidades") < LIMIAR_VALOR_MEDIO_PORTAB)))
n_susp = pt.filter("fl_qtd_portab_suspeita").count()
registra("silver.portabilidade", "Acurácia", f"valor médio por portabilidade >= R$ {LIMIAR_VALOR_MEDIO_PORTAB}", "linhas com quantidade implausível", n_susp, 0, "ALERTA",
         "sinalizar; QUANTIDADE não é usada nas análises")
display(pt.filter("fl_qtd_portab_suspeita").orderBy(F.desc("qt_portabilidades")).limit(10))
neg = pt.filter("vl_portabilidade < 0").count()
registra("silver.portabilidade", "Validade", "valor >= 0", "linhas negativas", neg, 0, "OK" if neg == 0 else "ALERTA", "manter e sinalizar")

zero = F.lit(0).cast("decimal(20,2)")
portab = (pt.groupBy("cod_empresa", "mes_ref", "ano_mes", F.col("tipoplano").alias("cod_produto"))
          .agg(F.sum(F.when(F.col("tipo_transferencia") == "ACEITA", F.col("vl_portabilidade")).otherwise(zero)).alias("vl_portab_aceita"),
               F.sum(F.when(F.col("tipo_transferencia") == "CEDIDA", F.col("vl_portabilidade")).otherwise(zero)).alias("vl_portab_cedida"),
               F.max("fl_qtd_portab_suspeita").alias("fl_qtd_portab_suspeita")))
teste_unicidade(portab, ["cod_empresa", "mes_ref", "cod_produto"], "silver.portabilidade")
portab.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.portabilidade")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Coerência da direção dos fluxos no agregado de mercado
# MAGIC Se `R`=aceita e `D`=cedida e a portabilidade ocorre majoritariamente entre entidades do próprio mercado, os totais anuais aceitos e cedidos devem ser próximos.

# COMMAND ----------

simetria = (spark.table(f"{S}.portabilidade").groupBy(F.year("mes_ref").alias("ano"))
            .agg(F.sum("vl_portab_aceita").alias("aceita"), F.sum("vl_portab_cedida").alias("cedida"))
            .withColumn("dif_pct", F.round(100 * (F.col("aceita") - F.col("cedida")) / F.col("cedida"), 2)).orderBy("ano"))
display(simetria)
maxdif = simetria.filter("ano BETWEEN 2014 AND 2025").agg(F.max(F.abs("dif_pct"))).first()[0]
registra("silver.portabilidade", "Acurácia (coerência)", "Σ aceita ≈ Σ cedida no mercado (2014–2025)", "maior |diferença %| anual", maxdif, "pequena (<15%)",
         "OK" if maxdif < 15 else "ALERTA", "diferença residual = transferências com entidades fora do escopo (hipótese)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. `silver.pmbac` (estoque — Provisão Matemática de Benefícios a Conceder)
# MAGIC **Problemas:** 100% das chaves com espaços à direita; meses com PMBaC = 0.
# MAGIC **Tratamento:** trim, tipagem, união VGBL/PGBL; zeros mantidos com flag. **Teste de compatibilidade com a taxa de resgate:** todo resgate precisa ter estoque no mesmo empresa×mês.

# COMMAND ----------

def le_fundos(tabela, produto):
    return (spark.table(f"{B}.{tabela}")
            .select(F.trim("coenti").alias("cod_empresa"), F.trim("damesano").alias("ano_mes"), mes_ref("damesano").alias("mes_ref"),
                    F.lit(produto).alias("cod_produto"), dec("fundos").alias("vl_pmbac")))
pmbac = le_fundos("ses_vgbl_fundos", "VGBL").unionByName(le_fundos("ses_pgbl_fundos", "PGBL")).withColumn("fl_pmbac_zero", F.col("vl_pmbac") == 0)
registra("silver.pmbac", "Completude", "PMBaC > 0", "empresa×mês com PMBaC = 0", pmbac.filter("fl_pmbac_zero").count(), "—", "INFO", "manter e sinalizar")
neg = pmbac.filter("vl_pmbac < 0").count()
registra("silver.pmbac", "Validade", "PMBaC >= 0", "linhas negativas", neg, 0, "OK" if neg == 0 else "ALERTA", "manter e sinalizar")
teste_unicidade(pmbac, ["cod_empresa", "mes_ref", "cod_produto"], "silver.pmbac")
pmbac.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.pmbac")

sem_estoque = (spark.table(f"{S}.resgates").filter("mes_ref >= '2014-01-01'")
               .join(spark.table(f"{S}.pmbac"), ["cod_empresa", "mes_ref", "cod_produto"], "left_anti")
               .agg(F.count("*").alias("n"), F.sum("vl_resgate").alias("v")).first())
registra("silver.pmbac", "Integridade", "resgate (2014+) tem PMBaC no mesmo empresa×mês×produto", "pares sem estoque / valor de resgate (R$)",
         f"{sem_estoque['n']} / {sem_estoque['v']}", "valor ≈ 0", "OK" if (sem_estoque["v"] or 0) < 1e6 else "ALERTA", "PMBaC compatível com a taxa de resgate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Conciliação PGBL entre fontes (`ses_pgbl_uf`)
# MAGIC
# MAGIC A tabela `ses_pgbl_uf` **não entra nas análises**; é usada apenas como **segunda fonte** para medir a consistência das contribuições e dos resgates de PGBL.
# MAGIC
# MAGIC - Conciliação A: `ses_contrib_benef` (PGBL) × Σ `CONTRIB` de `ses_pgbl_uf`, por empresa×mês.
# MAGIC - Conciliação B: `silver.resgates` (PGBL) × Σ `RESGPAGO` de `ses_pgbl_uf`, por empresa×mês.
# MAGIC
# MAGIC **Interpretação:** as fontes vêm de quadros diferentes do FIP; divergência pode decorrer de conceito, de recargas em datas diferentes ou de preenchimento de apenas um quadro. **Nenhuma das duas é "corrigida"**: a conciliação mede a confiabilidade e a `ses_contrib_benef` segue como fonte oficial das análises.

# COMMAND ----------

uf = spark.table(f"{B}.ses_pgbl_uf")
total_uf = uf.count()
for c in ["BENEFPAGO", "NUMBENEF"]:
    nulos = uf.filter(F.col(c).isNull() | (F.trim(F.col(c)) == "")).count()
    registra("bronze.ses_pgbl_uf", "Completude", f"{c} preenchido", "% nulo/vazio", round(100 * nulos / total_uf, 1), "—", "ALERTA" if nulos == total_uf else "OK",
             "coluna 100% vazia: não utilizada")

uf_emp = (uf.select(F.trim("COENTI").alias("cod_empresa"), mes_ref("DAMESANO").alias("mes_ref"), dec("CONTRIB").alias("c"), dec("RESGPAGO").alias("r"))
          .groupBy("cod_empresa", "mes_ref").agg(F.sum("c").alias("contrib_uf"), F.sum("r").alias("resg_uf")))

def concilia(nome, a, b):
    j = (a.join(b, ["cod_empresa", "mes_ref"], "full_outer").filter("mes_ref >= '2014-01-01'")
         .withColumn("vl_diferenca", F.coalesce("vl_fonte_a", F.lit(0)) - F.coalesce("vl_fonte_b", F.lit(0)))
         .withColumn("pc_diferenca", F.abs("vl_diferenca") / F.greatest(F.abs(F.coalesce("vl_fonte_a", F.lit(0))), F.lit(1)))
         .withColumn("classe_conciliacao",
                     F.when(F.col("vl_fonte_b").isNull(), "SO_FONTE_A")
                      .when(F.col("vl_fonte_a").isNull(), "SO_FONTE_B")
                      .when(F.col("pc_diferenca") <= 0.001, "ATE_0,1%")
                      .when(F.col("pc_diferenca") <= 0.01, "ATE_1%")
                      .when(F.col("pc_diferenca") <= 0.05, "ATE_5%")
                      .otherwise("ACIMA_5%"))
         .withColumn("tipo_conciliacao", F.lit(nome)))
    return j.select("tipo_conciliacao", "cod_empresa", "mes_ref", "vl_fonte_a", "vl_fonte_b", "vl_diferenca", "pc_diferenca", "classe_conciliacao")

conc = concilia("CONTRIB_PGBL: contrib_benef x pgbl_uf",
                spark.table(f"{S}.contribuicoes").filter("cod_produto = 'PGBL'").select("cod_empresa", "mes_ref", F.col("vl_contribuicao").alias("vl_fonte_a")),
                uf_emp.select("cod_empresa", "mes_ref", F.col("contrib_uf").alias("vl_fonte_b"))
       ).unionByName(concilia("RESGATE_PGBL: resgates x pgbl_uf",
                spark.table(f"{S}.resgates").filter("cod_produto = 'PGBL'").select("cod_empresa", "mes_ref", F.col("vl_resgate").alias("vl_fonte_a")),
                uf_emp.select("cod_empresa", "mes_ref", F.col("resg_uf").alias("vl_fonte_b"))))
conc.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.dq_conciliacao_pgbl")

resumo_conc = (spark.table(f"{S}.dq_conciliacao_pgbl").groupBy("tipo_conciliacao", "classe_conciliacao").count()
               .withColumn("pct", F.round(100 * F.col("count") / F.sum("count").over(Window.partitionBy("tipo_conciliacao")), 1)))
display(resumo_conc.orderBy("tipo_conciliacao", "classe_conciliacao"))
for tipo in [r[0] for r in resumo_conc.select("tipo_conciliacao").distinct().collect()]:
    ok = resumo_conc.filter((F.col("tipo_conciliacao") == tipo) & F.col("classe_conciliacao").isin("ATE_0,1%", "ATE_1%")).agg(F.sum("pct")).first()[0] or 0
    registra("silver.dq_conciliacao_pgbl", "Acurácia (conciliação entre fontes)", tipo, "% de empresa×mês conciliados até 1%", round(ok, 1), "alto (referência)",
             "OK" if ok >= 90 else "ALERTA", "não corrigir; usar contrib_benef/resgates como fonte oficial e reportar como limitação")

# Divergências por ano (onde se concentram?)
display(spark.table(f"{S}.dq_conciliacao_pgbl")
        .groupBy("tipo_conciliacao", F.year("mes_ref").alias("ano"))
        .agg(F.round(100 * F.avg(F.col("classe_conciliacao").isin("ATE_0,1%", "ATE_1%").cast("int")), 1).alias("pct_conciliado_ate_1pct"))
        .orderBy("tipo_conciliacao", "ano"))

# COMMAND ----------

# MAGIC %md ## 9. Integridade referencial (empresas) e outliers

# COMMAND ----------

emp = spark.table(f"{S}.empresa")
for t in ["contribuicoes", "resgates", "portabilidade", "pmbac", "empresa_grupo_mensal"]:
    orf = spark.table(f"{S}.{t}").select("cod_empresa").distinct().join(emp, "cod_empresa", "left_anti").count()
    registra(f"silver.{t}", "Integridade referencial", "cod_empresa existe em silver.empresa", "empresas órfãs", orf, 0,
             "OK" if orf == 0 else ("ALERTA" if t == "empresa_grupo_mensal" else "FALHA"), "incluir empresa desconhecida na dimensão / investigar")

# Outliers temporais: contribuição mensal > 5x a média dos 12 meses anteriores da própria empresa (e > R$ 50 mi). Não removidos.
w = Window.partitionBy("cod_empresa", "cod_produto").orderBy("mes_ref").rowsBetween(-12, -1)
out = (spark.table(f"{S}.contribuicoes").filter("mes_ref >= '2014-01-01'")
       .withColumn("media_12m_anterior", F.avg("vl_contribuicao").over(w))
       .filter("media_12m_anterior > 0 AND vl_contribuicao > 5 * media_12m_anterior AND vl_contribuicao > 50000000"))
registra("silver.contribuicoes", "Outliers", "contribuição > 5x média 12m anterior e > R$ 50 mi", "nº de empresa×mês atípicos", out.count(), "—", "INFO",
         "manter (podem ser eventos reais: aportes, incorporações); inspecionar")
display(out.withColumn("multiplo", F.round(F.col("vl_contribuicao") / F.col("media_12m_anterior"), 1)).orderBy(F.desc("multiplo")).limit(10))

# COMMAND ----------

# MAGIC %md ## 10. Persistência dos resultados de qualidade e da quarentena

# COMMAND ----------

(spark.createDataFrame(dq_linhas, "camada string, tabela string, dimensao string, regra string, metrica string, valor string, esperado string, status string, acao string")
 .withColumn("ts_execucao", F.current_timestamp())
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.dq_resultados"))

q_schema = "tabela_origem string, motivo string, registro_json string"
q = reduce(lambda a, b: a.unionByName(b), quarentenas) if quarentenas else spark.createDataFrame([], q_schema)
q.withColumn("ts_execucao", F.current_timestamp()).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{S}.quarentena")

# COMMAND ----------

# MAGIC %md ### Resultado consolidado dos testes de qualidade

# COMMAND ----------

display(spark.table(f"{S}.dq_resultados").orderBy("tabela", "dimensao"))

# COMMAND ----------

display(spark.table(f"{S}.quarentena"))

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {S}"))
