# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — Modelo dimensional e Catálogo de Dados
# MAGIC
# MAGIC **Modelo:** esquema estrela com **um fato de fluxo consolidado** (contribuições, resgates e portabilidade compartilham o mesmo grão: empresa × produto × mês) e **um fato de estoque** (PMBaC, fotografia de fim de mês).
# MAGIC
# MAGIC ```
# MAGIC                 dim_tempo (mes_ref)
# MAGIC                        │ 1:N
# MAGIC  dim_empresa ──1:N── fato_fluxo_previdencia ──N:1── dim_produto
# MAGIC  (cod_empresa)         │  (mes_ref, cod_empresa, cod_produto)
# MAGIC                        │ N:1
# MAGIC                   dim_grupo (cod_grupo)  ← grupo vigente no mês, resolvido via ponte_empresa_grupo_mes
# MAGIC
# MAGIC  fato_pmbac (mes_ref, cod_empresa, cod_produto) → mesmas dimensões
# MAGIC  ponte_empresa_grupo_mes (cod_empresa, mes_ref) → dim_empresa, dim_grupo, dim_tempo
# MAGIC ```
# MAGIC
# MAGIC **Período da Gold:** jan/2014 a jul/2026 (2014 é o primeiro ano completo após a mudança de granularidade de 12/2013; 2026 é parcial e só é comparado em base jan–jul).
# MAGIC
# MAGIC **Atribuição temporal de grupo econômico:** cada (empresa, mês) recebe o grupo informado em `ses_grupos_economicos` para aquele mês (EXATA); se o mês não existir, o último grupo conhecido anterior (HERDADA); sem histórico, `NAO_INFORMADO`.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "previdencia"
S, G = f"{CATALOGO}.silver", f"{CATALOGO}.gold"
INICIO, FIM = "2014-01-01", "2026-07-01"
ULTIMO_ANO_COMPLETO, ULTIMO_MES_YTD = 2025, 7

# Recriação idempotente: remove as tabelas Gold (fatos antes das dimensões, por causa das FKs)
for t in ["fato_fluxo_previdencia", "fato_pmbac", "ponte_empresa_grupo_mes", "dim_tempo", "dim_produto", "dim_empresa", "dim_grupo", "catalogo_dados"]:
    spark.sql(f"DROP TABLE IF EXISTS {G}.{t}")

def salva(df, tabela):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.{tabela}")

# COMMAND ----------

# MAGIC %md ## 1. Dimensões

# COMMAND ----------

dim_tempo = spark.sql(f"""
  SELECT mes_ref,
         CAST(date_format(mes_ref, 'yyyyMM') AS STRING) AS ano_mes,
         year(mes_ref)                                   AS ano,
         month(mes_ref)                                  AS mes,
         quarter(mes_ref)                                AS trimestre,
         year(mes_ref) <= {ULTIMO_ANO_COMPLETO}          AS fl_ano_completo,
         month(mes_ref) <= {ULTIMO_MES_YTD}              AS fl_periodo_ytd
  FROM (SELECT explode(sequence(DATE'{INICIO}', DATE'{FIM}', INTERVAL 1 MONTH)) AS mes_ref)
""")
salva(dim_tempo, "dim_tempo")

dim_produto = spark.createDataFrame([
    ("VGBL", "Vida Gerador de Benefício Livre", "Seguro de pessoas com cobertura por sobrevivência; na tributação de IR incide apenas sobre os rendimentos"),
    ("PGBL", "Plano Gerador de Benefício Livre", "Plano de previdência complementar aberta; contribuições dedutíveis da base do IR (declaração completa)"),
], "cod_produto string, nome_produto string, descricao_produto string")
salva(dim_produto, "dim_produto")

salva(spark.table(f"{S}.empresa"), "dim_empresa")

grupos = spark.sql(f"""
  SELECT cod_grupo, nome_grupo, fl_grupo_generico FROM (
    SELECT cod_grupo, nome_grupo, fl_grupo_generico,
           ROW_NUMBER() OVER (PARTITION BY cod_grupo ORDER BY mes_ref DESC) AS rn   -- nome mais recente do grupo
    FROM {S}.empresa_grupo_mensal) WHERE rn = 1
  UNION ALL SELECT 'NAO_INFORMADO', 'GRUPO NÃO INFORMADO', true
""")
salva(grupos, "dim_grupo")

# COMMAND ----------

# MAGIC %md ## 2. Ponte empresa × grupo × mês (atribuição temporal *as-of*)

# COMMAND ----------

ponte = spark.sql(f"""
WITH chaves AS (
  SELECT DISTINCT cod_empresa, mes_ref FROM (
    SELECT cod_empresa, mes_ref FROM {S}.contribuicoes
    UNION ALL SELECT cod_empresa, mes_ref FROM {S}.resgates
    UNION ALL SELECT cod_empresa, mes_ref FROM {S}.portabilidade
    UNION ALL SELECT cod_empresa, mes_ref FROM {S}.pmbac)
  WHERE mes_ref BETWEEN DATE'{INICIO}' AND DATE'{FIM}'),
candidatos AS (
  SELECT c.cod_empresa, c.mes_ref, g.cod_grupo, g.mes_ref AS mes_ref_grupo,
         ROW_NUMBER() OVER (PARTITION BY c.cod_empresa, c.mes_ref ORDER BY g.mes_ref DESC) AS rn
  FROM chaves c
  LEFT JOIN {S}.empresa_grupo_mensal g
    ON g.cod_empresa = c.cod_empresa AND g.mes_ref <= c.mes_ref)
SELECT cod_empresa, mes_ref,
       COALESCE(cod_grupo, 'NAO_INFORMADO') AS cod_grupo,
       CASE WHEN mes_ref_grupo = mes_ref THEN 'EXATA'
            WHEN mes_ref_grupo IS NOT NULL THEN 'HERDADA'
            ELSE 'NAO_INFORMADO' END AS tipo_atribuicao,
       CAST(months_between(mes_ref, mes_ref_grupo) AS INT) AS meses_defasagem
FROM candidatos WHERE rn = 1
""")
salva(ponte, "ponte_empresa_grupo_mes")
display(spark.table(f"{G}.ponte_empresa_grupo_mes").groupBy("tipo_atribuicao").agg(F.count("*").alias("empresa_mes"), F.max("meses_defasagem").alias("max_defasagem_meses")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fatos
# MAGIC
# MAGIC `fato_fluxo_previdencia` — grão **mês × empresa × produto**. União das chaves de contribuições, resgates e portabilidade (*full outer*): um mês com resgate mas sem contribuição continua existindo. Ausência de valor = 0, com flags indicando de qual fonte veio a informação.
# MAGIC
# MAGIC | Métrica | Fórmula |
# MAGIC |---|---|
# MAGIC | `vl_flcr` | `vl_contribuicao − vl_resgate` |
# MAGIC | `vl_portab_liquida` | `vl_portab_aceita − vl_portab_cedida` |
# MAGIC | `vl_captacao_liquida` | `vl_flcr + vl_portab_liquida` |

# COMMAND ----------

fato = spark.sql(f"""
WITH k AS (
  SELECT cod_empresa, mes_ref, cod_produto FROM {S}.contribuicoes
  UNION SELECT cod_empresa, mes_ref, cod_produto FROM {S}.resgates
  UNION SELECT cod_empresa, mes_ref, cod_produto FROM {S}.portabilidade)
SELECT k.mes_ref, k.cod_empresa, k.cod_produto, p.cod_grupo,
       CAST(COALESCE(c.vl_contribuicao, 0) AS DECIMAL(20,2))           AS vl_contribuicao,
       CAST(COALESCE(r.vl_resgate, 0) AS DECIMAL(20,2))                AS vl_resgate,
       CAST(COALESCE(r.vl_resgate_pag_programado, 0) AS DECIMAL(20,2)) AS vl_resgate_pag_programado,
       CAST(COALESCE(c.vl_contribuicao, 0) - COALESCE(r.vl_resgate, 0) AS DECIMAL(20,2)) AS vl_flcr,
       CAST(COALESCE(t.vl_portab_aceita, 0) AS DECIMAL(20,2))          AS vl_portab_aceita,
       CAST(COALESCE(t.vl_portab_cedida, 0) AS DECIMAL(20,2))          AS vl_portab_cedida,
       CAST(COALESCE(t.vl_portab_aceita, 0) - COALESCE(t.vl_portab_cedida, 0) AS DECIMAL(20,2)) AS vl_portab_liquida,
       CAST(COALESCE(c.vl_contribuicao, 0) - COALESCE(r.vl_resgate, 0)
            + COALESCE(t.vl_portab_aceita, 0) - COALESCE(t.vl_portab_cedida, 0) AS DECIMAL(20,2)) AS vl_captacao_liquida,
       c.cod_empresa IS NOT NULL AS fl_tem_contribuicao,
       r.cod_empresa IS NOT NULL AS fl_tem_resgate,
       t.cod_empresa IS NOT NULL AS fl_tem_portabilidade,
       COALESCE(c.fl_contribuicao_negativa, false) AS fl_contribuicao_negativa,
       COALESCE(t.fl_qtd_portab_suspeita, false)   AS fl_qtd_portab_suspeita
FROM k
LEFT JOIN {S}.contribuicoes c ON c.cod_empresa = k.cod_empresa AND c.mes_ref = k.mes_ref AND c.cod_produto = k.cod_produto
LEFT JOIN {S}.resgates      r ON r.cod_empresa = k.cod_empresa AND r.mes_ref = k.mes_ref AND r.cod_produto = k.cod_produto
LEFT JOIN {S}.portabilidade t ON t.cod_empresa = k.cod_empresa AND t.mes_ref = k.mes_ref AND t.cod_produto = k.cod_produto
JOIN {G}.ponte_empresa_grupo_mes p ON p.cod_empresa = k.cod_empresa AND p.mes_ref = k.mes_ref
WHERE k.mes_ref BETWEEN DATE'{INICIO}' AND DATE'{FIM}'
""")
salva(fato, "fato_fluxo_previdencia")

fato_pmbac = spark.sql(f"""
SELECT m.mes_ref, m.cod_empresa, m.cod_produto, p.cod_grupo, m.vl_pmbac
FROM {S}.pmbac m
JOIN {G}.ponte_empresa_grupo_mes p ON p.cod_empresa = m.cod_empresa AND p.mes_ref = m.mes_ref
WHERE m.mes_ref BETWEEN DATE'{INICIO}' AND DATE'{FIM}'
""")
salva(fato_pmbac, "fato_pmbac")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Chaves primárias e estrangeiras (constraints informativas do Unity Catalog)
# MAGIC As constraints documentam o modelo no catálogo (e aparecem no Catalog Explorer). A validade delas é garantida pelos testes da seção 5.

# COMMAND ----------

PKS = {
    "dim_tempo": ["mes_ref"], "dim_produto": ["cod_produto"], "dim_empresa": ["cod_empresa"], "dim_grupo": ["cod_grupo"],
    "ponte_empresa_grupo_mes": ["cod_empresa", "mes_ref"],
    "fato_fluxo_previdencia": ["mes_ref", "cod_empresa", "cod_produto"],
    "fato_pmbac": ["mes_ref", "cod_empresa", "cod_produto"],
}
FKS = {
    "fato_fluxo_previdencia": [("mes_ref", "dim_tempo"), ("cod_empresa", "dim_empresa"), ("cod_produto", "dim_produto"), ("cod_grupo", "dim_grupo")],
    "fato_pmbac": [("mes_ref", "dim_tempo"), ("cod_empresa", "dim_empresa"), ("cod_produto", "dim_produto"), ("cod_grupo", "dim_grupo")],
    "ponte_empresa_grupo_mes": [("cod_empresa", "dim_empresa"), ("cod_grupo", "dim_grupo"), ("mes_ref", "dim_tempo")],
}
status_constraints = []
def executa(sql, descricao):
    try:
        spark.sql(sql); status_constraints.append((descricao, "OK", ""))
    except Exception as e:
        status_constraints.append((descricao, "NÃO APLICADA", str(e)[:200]))

for t, cols in PKS.items():
    for c in cols:
        executa(f"ALTER TABLE {G}.{t} ALTER COLUMN {c} SET NOT NULL", f"{t}.{c} NOT NULL")
    executa(f"ALTER TABLE {G}.{t} ADD CONSTRAINT pk_{t} PRIMARY KEY ({', '.join(cols)})", f"PK {t}({', '.join(cols)})")
for t, fks in FKS.items():
    for c, d in fks:
        executa(f"ALTER TABLE {G}.{t} ADD CONSTRAINT fk_{t}_{c} FOREIGN KEY ({c}) REFERENCES {G}.{d}({PKS[d][0]})", f"FK {t}.{c} -> {d}")
display(spark.createDataFrame(status_constraints, "constraint string, status string, detalhe string"))

# COMMAND ----------

# MAGIC %md ## 5. Testes de qualidade da Gold (unicidade, integridade referencial, conservação de totais)

# COMMAND ----------

dq = []
def registra(tabela, dimensao, regra, metrica, valor, esperado, ok, acao):
    dq.append(("GOLD", tabela, dimensao, regra, metrica, str(valor), str(esperado), "OK" if ok else "FALHA", acao))

for t, cols in PKS.items():
    df = spark.table(f"{G}.{t}")
    dup = df.count() - df.select(*cols).distinct().count()
    registra(f"gold.{t}", "Unicidade", f"PK {cols}", "duplicados", dup, 0, dup == 0, "falhar")
for t, fks in FKS.items():
    for c, d in fks:
        orf = spark.table(f"{G}.{t}").join(spark.table(f"{G}.{d}"), c, "left_anti").count()
        registra(f"gold.{t}", "Integridade referencial", f"{c} existe em {d}", "órfãos", orf, 0, orf == 0, "falhar")

# Conservação: totais da Gold = totais da Silver no mesmo período (nenhum valor perdido/duplicado nos joins)
periodo = f"mes_ref BETWEEN DATE'{INICIO}' AND DATE'{FIM}'"
pares = [("vl_contribuicao", "contribuicoes", "vl_contribuicao"), ("vl_resgate", "resgates", "vl_resgate"),
         ("vl_portab_aceita", "portabilidade", "vl_portab_aceita"), ("vl_portab_cedida", "portabilidade", "vl_portab_cedida")]
fg = spark.table(f"{G}.fato_fluxo_previdencia")
for col_g, t_s, col_s in pares:
    vg = fg.agg(F.sum(col_g)).first()[0]
    vs = spark.table(f"{S}.{t_s}").filter(periodo).agg(F.sum(col_s)).first()[0]
    registra("gold.fato_fluxo_previdencia", "Acurácia (conservação)", f"Σ {col_g} Gold = Σ Silver", "diferença (R$)", vg - vs, 0, vg == vs, "falhar")
vg = spark.table(f"{G}.fato_pmbac").agg(F.sum("vl_pmbac")).first()[0]
vs = spark.table(f"{S}.pmbac").filter(periodo).agg(F.sum("vl_pmbac")).first()[0]
registra("gold.fato_pmbac", "Acurácia (conservação)", "Σ vl_pmbac Gold = Σ Silver", "diferença (R$)", vg - vs, 0, vg == vs, "falhar")

atrib = spark.table(f"{G}.ponte_empresa_grupo_mes").groupBy("tipo_atribuicao").count().collect()
registra("gold.ponte_empresa_grupo_mes", "Completude", "grupo atribuído a toda empresa×mês", "empresa×mês por tipo de atribuição",
         {r["tipo_atribuicao"]: r["count"] for r in atrib}, "NAO_INFORMADO ≈ 0", True, "reportar HERDADA/NAO_INFORMADO")

dq_df = spark.createDataFrame(dq, "camada string, tabela string, dimensao string, regra string, metrica string, valor string, esperado string, status string, acao string") \
             .withColumn("ts_execucao", F.current_timestamp())
dq_df.write.mode("append").saveAsTable(f"{S}.dq_resultados")
display(dq_df)
if dq_df.filter("status = 'FALHA'").count() > 0:
    raise ValueError("Testes da Gold falharam — ver tabela acima.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Catálogo de dados
# MAGIC
# MAGIC Para cada tabela e coluna das camadas Silver e Gold:
# MAGIC 1. descrição e **linhagem** são gravadas como `COMMENT` no **Unity Catalog** (visíveis no Catalog Explorer);
# MAGIC 2. o **tipo** vem do próprio schema;
# MAGIC 3. o **domínio** é **calculado a partir dos dados** (mín./máx. para números e datas; categorias para textos com poucos valores);
# MAGIC 4. tudo é consolidado na tabela `gold.catalogo_dados`.

# COMMAND ----------

TABELAS = {
  # tabela: (descrição, linhagem)
  "silver.empresa": ("Cadastro de empresas supervisionadas pela SUSEP.", "bronze.ses_cias → trim; colunas Cogrupo/Nogrupo (100% vazias) excluídas."),
  "silver.empresa_grupo_mensal": ("Grupo econômico de cada empresa em cada mês (retrato mensal).", "bronze.ses_grupos_economicos → trim; mês inválido e grupo vazio em quarentena; duplicatas exatas removidas."),
  "silver.contribuicoes": ("Contribuições e benefícios de VGBL e PGBL por empresa e mês.", "bronze.ses_contrib_benef → filtro tipoProd ∈ {VGBL, PGBL}; vírgula→ponto; DECIMAL(20,2); AAAAMM→DATE; flag de negativos."),
  "silver.resgates": ("Resgates de VGBL e PGBL por empresa e mês.", "bronze.ses_vgbl_resgates ∪ bronze.ses_pgbl_resgates → duplicatas exatas (soma zero) removidas; sub-linhas pós-12/2013 somadas; vl_resgate = total + parcial."),
  "silver.portabilidade": ("Portabilidade aceita e cedida de VGBL e PGBL por empresa e mês.", "bronze.ses_transferenciasexternas → TIPOTRANSF R→ACEITA, D→CEDIDA (demais em quarentena); TIPOPLANO ∈ {VGBL, PGBL}; pivot aceita/cedida."),
  "silver.pmbac": ("Provisão Matemática de Benefícios a Conceder (fundos) de VGBL e PGBL, fim de mês.", "bronze.ses_vgbl_fundos ∪ bronze.ses_pgbl_fundos → trim; DECIMAL; AAAAMM→DATE."),
  "silver.dq_conciliacao_pgbl": ("Conciliação de contribuições e resgates de PGBL entre duas fontes do SES (evidência de qualidade).", "silver.contribuicoes/silver.resgates × bronze.ses_pgbl_uf (Σ por empresa×mês), 2014+."),
  "silver.dq_resultados": ("Resultado de todos os testes de qualidade (Silver e Gold).", "Gerada pelos notebooks 02 e 03."),
  "silver.quarentena": ("Registros não interpretáveis com segurança, preservados em JSON.", "Gerada pelo notebook 02."),
  "gold.dim_tempo": ("Dimensão de meses de jan/2014 a jul/2026.", "Gerada por sequence(); flags de ano completo e de período YTD (jan–jul)."),
  "gold.dim_produto": ("Dimensão de produtos do escopo (VGBL, PGBL).", "Definida no notebook 03."),
  "gold.dim_empresa": ("Dimensão de empresas.", "silver.empresa."),
  "gold.dim_grupo": ("Dimensão de grupos econômicos (nome mais recente) + NAO_INFORMADO.", "silver.empresa_grupo_mensal."),
  "gold.ponte_empresa_grupo_mes": ("Grupo vigente para cada empresa em cada mês (atribuição as-of).", "chaves dos fatos × silver.empresa_grupo_mensal (último grupo com mês ≤ mês do fato)."),
  "gold.fato_fluxo_previdencia": ("Fato de fluxos mensais por empresa e produto: contribuições, resgates, FLCR, portabilidade e captação líquida.", "silver.contribuicoes ⟗ silver.resgates ⟗ silver.portabilidade + ponte_empresa_grupo_mes; jan/2014–jul/2026."),
  "gold.fato_pmbac": ("Fato de estoque de PMBaC de fim de mês por empresa e produto.", "silver.pmbac + ponte_empresa_grupo_mes; jan/2014–jul/2026."),
}
COLUNAS = {
  "cod_empresa": "Código FIP da empresa na SUSEP (5 dígitos, texto).",
  "nome_empresa": "Razão social da empresa (Ses_cias).",
  "nome_empresa_no_mes": "Nome da empresa informado na tabela de grupos naquele mês.",
  "cod_grupo": "Código do grupo econômico (99999 = 'OUTROS GRUPOS', genérico; NAO_INFORMADO = sem histórico).",
  "nome_grupo": "Nome do grupo econômico.",
  "fl_grupo_generico": "Verdadeiro para o código 99999 (não é um grupo real; no HHI cada empresa conta como unidade própria).",
  "mes_ref": "Mês de referência (1º dia do mês), derivado de damesano (AAAAMM).",
  "ano_mes": "Mês de referência no formato original AAAAMM.",
  "cod_produto": "Produto: VGBL ou PGBL.",
  "vl_contribuicao": "Contribuições no mês (R$ nominais). Origem: contrib.",
  "vl_beneficio": "Benefícios pagos no mês (R$ nominais). Origem: benef. Fora das métricas do projeto.",
  "fl_contribuicao_negativa": "Verdadeiro se a contribuição informada é negativa (estorno/ajuste). Mantida nos totais.",
  "vl_resgate_total": "Soma de resg_total (resgates totais) no mês.",
  "vl_resgate_parcial": "Soma de resg_parcial (resgates parciais) no mês.",
  "vl_resgate": "Resgates do projeto = resg_total + resg_parcial (R$ nominais).",
  "vl_resgate_pag_programado": "Soma de Resg_Pag_programado (coluna não documentada pela SUSEP); fora de vl_resgate.",
  "qt_linhas_origem": "Nº de linhas do arquivo de origem somadas na chave (1 até 11/2013; várias a partir de 12/2013).",
  "vl_portab_aceita": "Portabilidade aceita/recebida (TIPOTRANSF = R), R$ nominais.",
  "vl_portab_cedida": "Portabilidade cedida/enviada (TIPOTRANSF = D), R$ nominais.",
  "vl_portab_liquida": "Saldo líquido de portabilidade = aceita − cedida.",
  "fl_qtd_portab_suspeita": "Verdadeiro se alguma linha de origem tem valor médio por portabilidade < R$ 100 (QUANTIDADE implausível).",
  "vl_pmbac": "Provisão Matemática de Benefícios a Conceder aplicada em fundos, saldo de fim de mês (R$ nominais).",
  "fl_pmbac_zero": "Verdadeiro se a PMBaC informada é zero.",
  "vl_flcr": "FLCR — fluxo líquido de contribuições e resgates = vl_contribuicao − vl_resgate.",
  "vl_captacao_liquida": "Captação líquida (conceito de mercado) = vl_flcr + vl_portab_liquida.",
  "fl_tem_contribuicao": "Verdadeiro se a chave existe em silver.contribuicoes.",
  "fl_tem_resgate": "Verdadeiro se a chave existe em silver.resgates.",
  "fl_tem_portabilidade": "Verdadeiro se a chave existe em silver.portabilidade.",
  "ano": "Ano civil.", "mes": "Mês (1–12).", "trimestre": "Trimestre (1–4).",
  "fl_ano_completo": "Verdadeiro para anos com 12 meses na base (2014–2025).",
  "fl_periodo_ytd": "Verdadeiro para meses jan–jul (base de comparação com 2026).",
  "nome_produto": "Nome do produto.", "descricao_produto": "Descrição do produto.",
  "tipo_atribuicao": "EXATA (grupo do próprio mês), HERDADA (último grupo anterior) ou NAO_INFORMADO.",
  "meses_defasagem": "Meses entre o mês do fato e o mês do grupo utilizado (0 = exata).",
  "tipo_conciliacao": "Qual par de fontes está sendo conciliado.",
  "vl_fonte_a": "Valor da fonte oficial (contrib_benef ou arquivo de resgates).",
  "vl_fonte_b": "Valor da segunda fonte (Σ ses_pgbl_uf).",
  "vl_diferenca": "vl_fonte_a − vl_fonte_b.",
  "pc_diferenca": "|diferença| ÷ |vl_fonte_a| (fração).",
  "classe_conciliacao": "Faixa de conciliação (ATE_0,1%, ATE_1%, ATE_5%, ACIMA_5%, SO_FONTE_A, SO_FONTE_B).",
  "camada": "Camada em que o teste foi executado.", "tabela": "Tabela testada.", "dimensao": "Dimensão de qualidade.",
  "regra": "Regra testada.", "metrica": "Métrica calculada.", "valor": "Valor obtido.", "esperado": "Valor esperado.",
  "status": "OK, ALERTA, FALHA ou INFO.", "acao": "Ação em caso de falha/alerta.", "ts_execucao": "Momento da execução.",
  "tabela_origem": "Arquivo/tabela de onde veio o registro.", "motivo": "Motivo da quarentena.", "registro_json": "Registro original em JSON.",
}

esc = lambda s: s.replace("'", "\\'")
catalogo = []
for tab, (desc, lin) in TABELAS.items():
    nome = f"{CATALOGO}.{tab}"
    spark.sql(f"COMMENT ON TABLE {nome} IS '{esc(desc + ' Linhagem: ' + lin)}'")
    df = spark.table(nome)
    tipos = dict(df.dtypes)
    # domínio calculado a partir dos dados
    aggs = []
    for c, t in df.dtypes:
        if c == "registro_json":
            continue
        if t.startswith(("decimal", "int", "bigint", "double", "date", "timestamp")):
            aggs += [F.min(c).cast("string").alias(f"{c}__min"), F.max(c).cast("string").alias(f"{c}__max")]
        else:
            aggs += [F.countDistinct(c).alias(f"{c}__n"), F.slice(F.sort_array(F.collect_set(F.col(c).cast("string"))), 1, 12).alias(f"{c}__vals")]
        aggs.append(F.round(100 * F.avg(F.col(c).isNull().cast("int")), 2).alias(f"{c}__pnull"))
    stats = df.agg(*aggs).first().asDict() if aggs else {}
    for c, t in df.dtypes:
        descricao = COLUNAS.get(c, "")
        if descricao:
            spark.sql(f"ALTER TABLE {nome} ALTER COLUMN {c} COMMENT '{esc(descricao)}'")
        if c == "registro_json":
            dominio, pnull = "JSON", None
        elif f"{c}__min" in stats:
            dominio = f"[{stats[c + '__min']} ; {stats[c + '__max']}]"
            pnull = stats[f"{c}__pnull"]
        else:
            n, vals = stats[f"{c}__n"], stats[f"{c}__vals"]
            dominio = ("{" + ", ".join(vals) + "}") if n <= 12 else f"{n} valores distintos (ex.: {', '.join(vals[:3])} ...)"
            pnull = stats[f"{c}__pnull"]
        catalogo.append((tab.split(".")[0].upper(), tab, desc, lin, c, descricao, t, dominio, pnull))

cat_df = spark.createDataFrame(catalogo, "camada string, tabela string, descricao_tabela string, linhagem string, coluna string, descricao_coluna string, tipo string, dominio_observado string, pct_nulos double")
cat_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{G}.catalogo_dados")
spark.sql(f"COMMENT ON TABLE {G}.catalogo_dados IS 'Catálogo de dados do projeto: descrição, tipo, domínio observado, % nulos e linhagem de cada coluna das camadas Silver e Gold.'")
sem_desc = cat_df.filter("descricao_coluna = ''").count()
print(f"Colunas sem descrição no catálogo: {sem_desc}")

# COMMAND ----------

# MAGIC %md ### Catálogo de dados — modelo Gold

# COMMAND ----------

display(spark.table(f"{G}.catalogo_dados").filter("camada = 'GOLD'").orderBy("tabela", "coluna"))

# COMMAND ----------

# MAGIC %md ### Catálogo de dados — Silver

# COMMAND ----------

display(spark.table(f"{G}.catalogo_dados").filter("camada = 'SILVER'").orderBy("tabela", "coluna"))

# COMMAND ----------

# MAGIC %md ### Evidências do modelo: tabelas, schema e constraints no Unity Catalog

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {G}"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {G}.fato_fluxo_previdencia"))

# COMMAND ----------

try:
    display(spark.sql(f"""
      SELECT tc.table_name, tc.constraint_name, tc.constraint_type
      FROM {CATALOGO}.information_schema.table_constraints tc
      WHERE tc.table_schema = 'gold' ORDER BY tc.table_name, tc.constraint_type"""))
except Exception as e:
    print("information_schema indisponível:", str(e)[:200])

# COMMAND ----------

display(spark.table(f"{G}.fato_fluxo_previdencia").orderBy(F.desc("mes_ref"), F.desc("vl_contribuicao")).limit(20))
