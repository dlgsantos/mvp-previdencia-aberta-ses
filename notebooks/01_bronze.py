# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — ingestão dos arquivos do SES/SUSEP
# MAGIC
# MAGIC **Projeto:** Fluxos da previdência complementar aberta no Brasil (2014–2026): contribuições, resgates e portabilidade de VGBL e PGBL por grupo econômico
# MAGIC
# MAGIC **Objetivo desta etapa:** trazer os arquivos brutos para tabelas Delta **sem alterar o conteúdo**, acrescentando apenas metadados de rastreabilidade.
# MAGIC
# MAGIC | Item | Decisão |
# MAGIC |---|---|
# MAGIC | Fonte | SES — Sistema de Estatísticas da SUSEP. Base completa para download (`BaseCompleta.zip`), gerada em **21/09/2026**, dados até **07/2026** |
# MAGIC | Origem dos dados | Formulários de Informações Periódicas (FIP) enviados à SUSEP por seguradoras e EAPPs |
# MAGIC | Coleta | Download manual do ZIP em https://www2.susep.gov.br/menuestatistica/ses/principal.aspx → descompactação local → upload dos 9 CSVs para o Volume `previdencia.bronze.raw` (pasta `ses/`) |
# MAGIC | Por que upload manual | O Databricks Free Edition restringe o acesso de saída à internet; o upload para Volume é o caminho documentado pela plataforma |
# MAGIC | Formato de leitura | CSV, separador `;`, cabeçalho na 1ª linha, charset **windows-1252**, todas as colunas como **texto** |
# MAGIC | Transformação de conteúdo | **Nenhuma.** Números continuam com vírgula, chaves continuam com espaços, duplicatas continuam presentes |
# MAGIC
# MAGIC **Por que tudo como texto?** Os arquivos usam vírgula decimal e têm chaves com espaços. Qualquer inferência de tipo aqui já seria uma transformação (e poderia perder informação). A tipagem é feita — e testada — na Silver.
# MAGIC
# MAGIC **Observação sobre charset:** ler com `windows-1252` é *decodificação* do arquivo (como ele foi gravado), não alteração do conteúdo. O efeito de ler com o charset errado é demonstrado no notebook 02.

# COMMAND ----------

# MAGIC %md ## 1. Configuração

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "previdencia"
VOLUME_DIR = f"/Volumes/{CATALOGO}/bronze/raw/ses"
DATA_GERACAO_BASE_SES = "2026-09-21"   # data de geração informada na página do SES
MES_REFERENCIA_MAX = "202607"          # último mês disponível na base

# arquivo (como baixado do SES) -> (tabela bronze, descrição segundo a documentação oficial do SES)
ARQUIVOS = {
    "Ses_Contrib_Benef.csv":          ("ses_contrib_benef",          "Valores de contribuições e benefícios por empresa, mês e tipo de produto (PGBL, VGBL, PrevTrad)"),
    "Ses_vgbl_resgates.csv":          ("ses_vgbl_resgates",          "VGBL - Resgates por empresa e mês"),
    "ses_pgbl_resgates.csv":          ("ses_pgbl_resgates",          "PGBL: Resgates por empresa e mês"),
    "ses_transferenciasexternas.csv": ("ses_transferenciasexternas", "Previdência: Portabilidades Externas (tipo de transferência aceita/cedida) por empresa, mês e produto"),
    "Ses_vgbl_fundos.csv":            ("ses_vgbl_fundos",            "VGBL: Provisão Matemática de Benefícios a Conceder (Fundos) por empresa e mês"),
    "ses_pgbl_fundos.csv":            ("ses_pgbl_fundos",            "PGBL: Provisão Matemática de Benefícios a Conceder (Fundos) por empresa e mês"),
    "ses_pgbl_uf.csv":                ("ses_pgbl_uf",                "PGBL: Dados por UF (contribuições, resgates, participantes) por empresa e mês"),
    "Ses_cias.csv":                   ("ses_cias",                   "Cias do Mercado: cadastro de empresas supervisionadas"),
    "Ses_grupos_economicos.csv":      ("ses_grupos_economicos",      "Todo o Mercado: Grupos Econômicos por empresa e mês"),
}

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.bronze.raw")
print("Schemas e volume verificados.")

# COMMAND ----------

# MAGIC %md ## 2. Evidência dos arquivos brutos no Volume (camada *raw*)

# COMMAND ----------

arquivos_volume = dbutils.fs.ls(VOLUME_DIR)
display(spark.createDataFrame(
    [(f.name, round(f.size / 1024 / 1024, 2), f.path) for f in arquivos_volume],
    "arquivo string, tamanho_mb double, caminho string",
).orderBy("arquivo"))

# Localiza cada arquivo esperado sem depender de maiúsculas/minúsculas no nome
por_nome = {f.name.lower(): f.path for f in arquivos_volume}
faltando = [a for a in ARQUIVOS if a.lower() not in por_nome]
if faltando:
    raise FileNotFoundError(f"Arquivos não encontrados em {VOLUME_DIR}: {faltando}")
CAMINHOS = {a: por_nome[a.lower()] for a in ARQUIVOS}
print("Todos os 9 arquivos esperados foram encontrados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingestão Bronze
# MAGIC
# MAGIC Metadados acrescentados a cada linha:
# MAGIC
# MAGIC | Coluna | Conteúdo | Finalidade |
# MAGIC |---|---|---|
# MAGIC | `_arquivo_origem` | caminho do arquivo no Volume | linhagem até o arquivo |
# MAGIC | `_data_geracao_base_ses` | 2026-09-21 | identifica o *snapshot* do SES (a SUSEP pode recarregar dados antigos) |
# MAGIC | `_mes_referencia_max` | 202607 | último mês contido na base |
# MAGIC | `_ts_ingestao` | timestamp da carga | auditoria |
# MAGIC | `_hash_linha` | SHA-256 do conteúdo original da linha | identificar a linha de origem e detectar duplicatas exatas |

# COMMAND ----------

def ingerir_bronze(arquivo: str, tabela: str, descricao: str):
    caminho = CAMINHOS[arquivo]
    df = (spark.read.format("csv")
          .option("header", "true")
          .option("sep", ";")
          .option("encoding", "windows-1252")
          .option("inferSchema", "false")
          .load(caminho))
    colunas_originais = df.columns
    df = (df
          .withColumn("_arquivo_origem", F.col("_metadata.file_path"))
          .withColumn("_data_geracao_base_ses", F.lit(DATA_GERACAO_BASE_SES).cast("date"))
          .withColumn("_mes_referencia_max", F.lit(MES_REFERENCIA_MAX))
          .withColumn("_ts_ingestao", F.current_timestamp())
          .withColumn("_hash_linha", F.sha2(F.concat_ws("|", *[F.coalesce(F.col(f"`{c}`"), F.lit("")) for c in colunas_originais]), 256)))
    nome = f"{CATALOGO}.bronze.{tabela}"
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nome)
    spark.sql(f"COMMENT ON TABLE {nome} IS 'BRONZE (dado como veio). {descricao}. Origem: arquivo {arquivo} da base completa do SES/SUSEP gerada em {DATA_GERACAO_BASE_SES}. Todas as colunas de negócio em texto, sem transformação.'")

    # Teste de completude da carga: nº de linhas da tabela = nº de linhas de dados do arquivo (linhas de texto - cabeçalho)
    linhas_arquivo = spark.read.text(caminho).filter(F.length(F.trim("value")) > 0).count() - 1
    linhas_tabela = spark.table(nome).count()
    return (tabela, arquivo, len(colunas_originais), ", ".join(colunas_originais), linhas_arquivo, linhas_tabela,
            "OK" if linhas_arquivo == linhas_tabela else "DIVERGENTE")

resumo = [ingerir_bronze(a, t, d) for a, (t, d) in ARQUIVOS.items()]

# COMMAND ----------

# MAGIC %md ## 4. Evidências da carga Bronze

# COMMAND ----------

# MAGIC %md ### 4.1 Contagem de linhas: arquivo × tabela (nenhuma linha perdida na ingestão)

# COMMAND ----------

display(spark.createDataFrame(
    resumo,
    "tabela string, arquivo string, n_colunas int, colunas_originais string, linhas_no_arquivo long, linhas_na_tabela long, status string",
))

# COMMAND ----------

# MAGIC %md ### 4.2 Tabelas criadas no schema `bronze`

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOGO}.bronze"))

# COMMAND ----------

# MAGIC %md ### 4.3 Schema de uma tabela Bronze (todas as colunas de negócio como `string`)

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOGO}.bronze.ses_contrib_benef"))

# COMMAND ----------

# MAGIC %md ### 4.4 Amostras — observe vírgula decimal, espaços nas chaves e nomes de colunas com caixas diferentes

# COMMAND ----------

for _, (tabela, _) in ARQUIVOS.items():
    print(f"=== {CATALOGO}.bronze.{tabela}")
    display(spark.table(f"{CATALOGO}.bronze.{tabela}").limit(5))
