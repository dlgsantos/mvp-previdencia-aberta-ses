# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Análises — respostas às perguntas de negócio
# MAGIC
# MAGIC Todas as consultas usam apenas a camada **Gold** (`fato_fluxo_previdencia`, `fato_pmbac` e dimensões).
# MAGIC
# MAGIC **Convenções:**
# MAGIC - valores em **R$ bilhões nominais** (sem correção pela inflação — limitação declarada);
# MAGIC - comparações anuais usam somente **anos completos (2014–2025)**;
# MAGIC - **2026 aparece apenas como YTD jan–jul**, comparado com jan–jul dos anos anteriores;
# MAGIC - FLCR = contribuições − resgates; captação líquida = FLCR + saldo líquido de portabilidade.

# COMMAND ----------

import matplotlib.pyplot as plt
from pyspark.sql import functions as F

CATALOGO = "previdencia"
G = f"{CATALOGO}.gold"
spark.sql(f"USE {G}")
CORES = {"VGBL": "#1f5aa6", "PGBL": "#d9822b"}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Como evoluíram contribuições, resgates e FLCR de VGBL e PGBL (2014–2025) e como jan–jul/2026 se compara a jan–jul dos anos anteriores?

# COMMAND ----------

q1_anual = spark.sql("""
SELECT t.ano, f.cod_produto,
       ROUND(SUM(f.vl_contribuicao) / 1e9, 2) AS contribuicoes_bi,
       ROUND(SUM(f.vl_resgate)      / 1e9, 2) AS resgates_bi,
       ROUND(SUM(f.vl_flcr)         / 1e9, 2) AS flcr_bi,
       ROUND(SUM(f.vl_resgate) / SUM(f.vl_contribuicao), 3) AS resgate_por_real_contribuido
FROM fato_fluxo_previdencia f JOIN dim_tempo t ON t.mes_ref = f.mes_ref
WHERE t.fl_ano_completo
GROUP BY t.ano, f.cod_produto
ORDER BY f.cod_produto, t.ano
""")
display(q1_anual)

# COMMAND ----------

pdf = q1_anual.toPandas()
fig, axes = plt.subplots(1, 2, figsize=(14, 4.5), sharex=True)
for ax, prod in zip(axes, ["VGBL", "PGBL"]):
    d = pdf[pdf.cod_produto == prod]
    ax.bar(d.ano - 0.2, d.contribuicoes_bi, width=0.4, label="Contribuições", color=CORES[prod])
    ax.bar(d.ano + 0.2, d.resgates_bi, width=0.4, label="Resgates", color="#9aa5b1")
    ax.plot(d.ano, d.flcr_bi, color="black", marker="o", label="FLCR")
    ax.axhline(0, color="grey", lw=0.8)
    ax.set_title(f"{prod} — contribuições, resgates e FLCR (R$ bi nominais)")
    ax.legend(); ax.grid(axis="y", alpha=0.3)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ### Q1 (YTD) — jan–jul de cada ano, incluindo 2026

# COMMAND ----------

q1_ytd = spark.sql("""
SELECT t.ano, f.cod_produto,
       ROUND(SUM(f.vl_contribuicao) / 1e9, 2) AS contribuicoes_jan_jul_bi,
       ROUND(SUM(f.vl_resgate)      / 1e9, 2) AS resgates_jan_jul_bi,
       ROUND(SUM(f.vl_flcr)         / 1e9, 2) AS flcr_jan_jul_bi
FROM fato_fluxo_previdencia f JOIN dim_tempo t ON t.mes_ref = f.mes_ref
WHERE t.fl_periodo_ytd AND t.ano >= 2021
GROUP BY t.ano, f.cod_produto
ORDER BY f.cod_produto, t.ano
""")
display(q1_ytd)

# COMMAND ----------

# MAGIC %md ### Q1 (mensal) — série recente com média móvel de 3 meses

# COMMAND ----------

q1_mensal = spark.sql("""
WITH m AS (
  SELECT f.mes_ref, f.cod_produto, SUM(f.vl_contribuicao)/1e9 AS contrib_bi, SUM(f.vl_resgate)/1e9 AS resg_bi, SUM(f.vl_flcr)/1e9 AS flcr_bi
  FROM fato_fluxo_previdencia f GROUP BY f.mes_ref, f.cod_produto)
SELECT mes_ref, cod_produto, ROUND(contrib_bi, 2) AS contribuicoes_bi, ROUND(resg_bi, 2) AS resgates_bi, ROUND(flcr_bi, 2) AS flcr_bi,
       ROUND(AVG(flcr_bi) OVER (PARTITION BY cod_produto ORDER BY mes_ref ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS flcr_mm3_bi
FROM m WHERE mes_ref >= '2022-01-01' ORDER BY cod_produto, mes_ref
""")
display(q1_mensal)
pdf = q1_mensal.toPandas()
fig, ax = plt.subplots(figsize=(14, 4.5))
for prod in ["VGBL", "PGBL"]:
    d = pdf[pdf.cod_produto == prod]
    ax.plot(d.mes_ref, d.flcr_bi, color=CORES[prod], alpha=0.35, label=f"{prod} FLCR mensal")
    ax.plot(d.mes_ref, d.flcr_mm3_bi, color=CORES[prod], lw=2.2, label=f"{prod} média móvel 3m")
ax.axhline(0, color="grey", lw=0.8); ax.set_title("FLCR mensal (R$ bi nominais), jan/2022–jul/2026"); ax.legend(); ax.grid(alpha=0.3)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação Q1:** *(preencher com os números obtidos acima)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Como evoluiu a taxa de resgate (resgates ÷ PMBaC média) de VGBL e PGBL?
# MAGIC Taxa do ano = Σ resgates do ano ÷ média dos saldos mensais de PMBaC do ano. Para 2026: jan–jul, **sem anualizar**, comparado com jan–jul dos anos anteriores.

# COMMAND ----------

q2 = spark.sql("""
WITH r AS (SELECT t.ano, f.cod_produto, t.fl_ano_completo, t.fl_periodo_ytd, f.mes_ref, SUM(f.vl_resgate) AS resg
           FROM fato_fluxo_previdencia f JOIN dim_tempo t ON t.mes_ref = f.mes_ref GROUP BY ALL),
     e AS (SELECT mes_ref, cod_produto, SUM(vl_pmbac) AS pmbac FROM fato_pmbac GROUP BY ALL),
     m AS (SELECT r.*, e.pmbac FROM r JOIN e ON e.mes_ref = r.mes_ref AND e.cod_produto = r.cod_produto)
SELECT ano, cod_produto, 'ANO_COMPLETO' AS base,
       ROUND(SUM(resg)/1e9, 1) AS resgates_bi, ROUND(AVG(pmbac)/1e9, 1) AS pmbac_media_bi, ROUND(100 * SUM(resg)/AVG(pmbac), 2) AS taxa_resgate_pct
FROM m WHERE fl_ano_completo GROUP BY ano, cod_produto
UNION ALL
SELECT ano, cod_produto, 'JAN_JUL' AS base,
       ROUND(SUM(resg)/1e9, 1), ROUND(AVG(pmbac)/1e9, 1), ROUND(100 * SUM(resg)/AVG(pmbac), 2)
FROM m WHERE fl_periodo_ytd AND ano >= 2021 GROUP BY ano, cod_produto
ORDER BY base, cod_produto, ano
""")
display(q2)
pdf = q2.filter("base = 'ANO_COMPLETO'").toPandas()
fig, ax = plt.subplots(figsize=(10, 4))
for prod in ["VGBL", "PGBL"]:
    d = pdf[pdf.cod_produto == prod]
    ax.plot(d.ano, d.taxa_resgate_pct, marker="o", color=CORES[prod], label=prod)
ax.set_title("Taxa anual de resgate (% da PMBaC média)"); ax.set_ylabel("%"); ax.legend(); ax.grid(alpha=0.3)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação Q2:** *(preencher)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3. Qual o grau de concentração das contribuições entre grupos econômicos (participação dos 5 maiores e HHI)?
# MAGIC - Unidade econômica = grupo vigente no mês. Empresas do código **99999 ("OUTROS GRUPOS")** ou sem grupo contam **cada uma como unidade própria**.
# MAGIC - HHI = Σ (participação × 100)², de 0 a 10.000. Unidades com contribuição anual ≤ 0 são excluídas do cálculo de participação.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW contrib_unidade_ano AS
SELECT t.ano, f.cod_produto,
       CASE WHEN f.cod_grupo IN ('99999', 'NAO_INFORMADO') THEN CONCAT('EMP_', f.cod_empresa) ELSE f.cod_grupo END AS unidade,
       CASE WHEN f.cod_grupo IN ('99999', 'NAO_INFORMADO') THEN CONCAT(e.nome_empresa, ' (sem grupo)') ELSE g.nome_grupo END AS nome_unidade,
       SUM(f.vl_contribuicao) AS contrib
FROM fato_fluxo_previdencia f
JOIN dim_tempo t ON t.mes_ref = f.mes_ref
JOIN dim_grupo g ON g.cod_grupo = f.cod_grupo
JOIN dim_empresa e ON e.cod_empresa = f.cod_empresa
WHERE t.fl_ano_completo
GROUP BY ALL
""")
q3 = spark.sql("""
WITH s AS (SELECT *, contrib / SUM(contrib) OVER (PARTITION BY ano, cod_produto) AS share,
                  ROW_NUMBER() OVER (PARTITION BY ano, cod_produto ORDER BY contrib DESC) AS pos
           FROM contrib_unidade_ano WHERE contrib > 0)
SELECT ano, cod_produto, COUNT(*) AS n_unidades,
       ROUND(100 * SUM(CASE WHEN pos <= 5 THEN share END), 1) AS share_top5_pct,
       ROUND(SUM(POWER(share * 100, 2)), 0) AS hhi
FROM s GROUP BY ano, cod_produto ORDER BY cod_produto, ano
""")
display(q3)
pdf = q3.toPandas()
fig, ax = plt.subplots(figsize=(10, 4))
for prod in ["VGBL", "PGBL"]:
    d = pdf[pdf.cod_produto == prod]
    ax.plot(d.ano, d.hhi, marker="o", color=CORES[prod], label=f"HHI {prod}")
ax.set_title("Índice Herfindahl-Hirschman das contribuições por grupo econômico"); ax.legend(); ax.grid(alpha=0.3)
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ### Q3 — os 5 maiores grupos em 2014 e em 2025

# COMMAND ----------

display(spark.sql("""
WITH s AS (SELECT *, contrib / SUM(contrib) OVER (PARTITION BY ano, cod_produto) AS share,
                  ROW_NUMBER() OVER (PARTITION BY ano, cod_produto ORDER BY contrib DESC) AS pos
           FROM contrib_unidade_ano WHERE contrib > 0)
SELECT cod_produto, ano, pos, nome_unidade, ROUND(contrib/1e9, 2) AS contribuicoes_bi, ROUND(100*share, 1) AS participacao_pct
FROM s WHERE pos <= 5 AND ano IN (2014, 2025) ORDER BY cod_produto, ano, pos
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação Q3:** *(preencher)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4. Qual o saldo líquido de portabilidade por grupo econômico e qual o seu tamanho em relação às contribuições do grupo?
# MAGIC Saldo líquido de portabilidade = aceita − cedida. No agregado do mercado ele tende a zero (transferência entre entidades); por isso é analisado **por unidade econômica** — grupo econômico vigente no mês, ou a própria empresa quando ela está no código genérico 99999 ("OUTROS GRUPOS"), mesma regra da Q3.

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TEMP VIEW grupo_periodo AS
SELECT CASE WHEN t.fl_ano_completo THEN CAST(t.ano AS STRING) END AS periodo_ano,
       CASE WHEN t.fl_periodo_ytd THEN CONCAT(CAST(t.ano AS STRING), '_JAN_JUL') END AS periodo_ytd,
       f.cod_produto, f.cod_grupo, g.nome_grupo,
       -- mesma unidade econômica da Q3: empresas do grupo genérico 99999 (ou sem grupo) contam como unidade própria
       CASE WHEN f.cod_grupo IN ('99999', 'NAO_INFORMADO') THEN CONCAT('EMP_', f.cod_empresa) ELSE f.cod_grupo END AS unidade,
       CASE WHEN f.cod_grupo IN ('99999', 'NAO_INFORMADO') THEN CONCAT(e.nome_empresa, ' (sem grupo)') ELSE g.nome_grupo END AS nome_unidade,
       f.vl_contribuicao, f.vl_resgate, f.vl_flcr, f.vl_portab_aceita, f.vl_portab_cedida, f.vl_portab_liquida, f.vl_captacao_liquida
FROM fato_fluxo_previdencia f
JOIN dim_tempo t ON t.mes_ref = f.mes_ref
JOIN dim_grupo g ON g.cod_grupo = f.cod_grupo
JOIN dim_empresa e ON e.cod_empresa = f.cod_empresa
""")

# Verificação: no total do mercado o saldo líquido de portabilidade é pequeno frente ao volume portado
display(spark.sql("""
SELECT periodo_ano AS ano, ROUND(SUM(vl_portab_aceita)/1e9, 1) AS aceita_bi, ROUND(SUM(vl_portab_cedida)/1e9, 1) AS cedida_bi,
       ROUND(SUM(vl_portab_liquida)/1e9, 2) AS saldo_liquido_mercado_bi
FROM grupo_periodo WHERE periodo_ano IS NOT NULL GROUP BY periodo_ano ORDER BY ano
"""))

# COMMAND ----------

q4 = spark.sql("""
SELECT nome_unidade, unidade,
       ROUND(SUM(vl_portab_aceita)/1e9, 2) AS aceita_bi, ROUND(SUM(vl_portab_cedida)/1e9, 2) AS cedida_bi,
       ROUND(SUM(vl_portab_liquida)/1e9, 2) AS saldo_liquido_portab_bi,
       ROUND(SUM(vl_contribuicao)/1e9, 2) AS contribuicoes_bi,
       ROUND(100 * SUM(vl_portab_liquida) / NULLIF(SUM(vl_contribuicao), 0), 1) AS saldo_portab_pct_contrib
FROM grupo_periodo
WHERE periodo_ano = '2025'
GROUP BY nome_unidade, unidade
HAVING SUM(vl_portab_aceita) + SUM(vl_portab_cedida) > 0
ORDER BY saldo_liquido_portab_bi DESC
""")
display(q4)
pdf = q4.toPandas()
pdf = pdf.reindex(pdf.saldo_liquido_portab_bi.abs().sort_values(ascending=False).index).head(15).sort_values("saldo_liquido_portab_bi")
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(pdf.nome_unidade, pdf.saldo_liquido_portab_bi, color=["#2e7d32" if v > 0 else "#c62828" for v in pdf.saldo_liquido_portab_bi])
ax.axvline(0, color="grey", lw=0.8)
ax.set_title("Saldo líquido de portabilidade por unidade econômica, 2025 (R$ bi, VGBL+PGBL)\n15 maiores em valor absoluto")
plt.tight_layout(); plt.show()

# COMMAND ----------

# MAGIC %md ### Q4 — evolução anual do saldo líquido de portabilidade dos grupos com maior volume portado

# COMMAND ----------

display(spark.sql("""
WITH top AS (SELECT unidade FROM grupo_periodo WHERE periodo_ano BETWEEN '2021' AND '2025'
             GROUP BY unidade ORDER BY SUM(vl_portab_aceita + vl_portab_cedida) DESC LIMIT 8)
SELECT periodo_ano AS ano, nome_unidade, ROUND(SUM(vl_portab_liquida)/1e9, 2) AS saldo_liquido_portab_bi
FROM grupo_periodo WHERE periodo_ano BETWEEN '2014' AND '2025' AND unidade IN (SELECT unidade FROM top)
GROUP BY ALL ORDER BY nome_unidade, ano
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação Q4:** *(preencher)*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5. Quanto o resultado e a posição de cada grupo mudam quando se passa do FLCR para a captação líquida?
# MAGIC Ranking dos grupos em 2025 (VGBL + PGBL) pelas duas métricas; `variacao_posicao` > 0 = o grupo sobe no ranking quando a portabilidade é incluída.

# COMMAND ----------

q5 = spark.sql("""
WITH g AS (
  SELECT nome_unidade, SUM(vl_flcr) AS flcr, SUM(vl_portab_liquida) AS portab, SUM(vl_captacao_liquida) AS captacao, SUM(vl_contribuicao) AS contrib
  FROM grupo_periodo WHERE periodo_ano = '2025'
  GROUP BY nome_unidade HAVING SUM(vl_contribuicao) > 0)
SELECT nome_unidade,
       ROUND(flcr/1e9, 2) AS flcr_bi, ROUND(portab/1e9, 2) AS saldo_portab_bi, ROUND(captacao/1e9, 2) AS captacao_liquida_bi,
       RANK() OVER (ORDER BY flcr DESC)     AS posicao_flcr,
       RANK() OVER (ORDER BY captacao DESC) AS posicao_captacao,
       RANK() OVER (ORDER BY flcr DESC) - RANK() OVER (ORDER BY captacao DESC) AS variacao_posicao,
       SIGN(flcr) * SIGN(captacao) < 0 AS muda_de_sinal   -- só conta troca real de sinal (zeros não contam)
FROM g ORDER BY posicao_captacao
""")
display(q5)

# COMMAND ----------

pdf = q5.toPandas().head(12)
fig, ax = plt.subplots(figsize=(11, 5.5))
y = range(len(pdf))
ax.barh([i + 0.2 for i in y], pdf.flcr_bi, height=0.4, label="FLCR", color="#9aa5b1")
ax.barh([i - 0.2 for i in y], pdf.captacao_liquida_bi, height=0.4, label="Captação líquida (FLCR + portabilidade)", color="#1f5aa6")
ax.set_yticks(list(y)); ax.set_yticklabels(pdf.nome_unidade); ax.invert_yaxis(); ax.axvline(0, color="grey", lw=0.8)
ax.set_title("FLCR × captação líquida por unidade econômica, 2025 (R$ bi, VGBL+PGBL) — 12 primeiros pela captação líquida"); ax.legend()
plt.tight_layout(); plt.show()

# COMMAND ----------

display(spark.sql("""
SELECT COUNT(*) AS grupos,
       SUM(CASE WHEN variacao_posicao <> 0 THEN 1 ELSE 0 END) AS grupos_que_mudam_de_posicao,
       SUM(CASE WHEN muda_de_sinal THEN 1 ELSE 0 END) AS grupos_que_mudam_de_sinal
FROM (
  SELECT RANK() OVER (ORDER BY flcr DESC) - RANK() OVER (ORDER BY captacao DESC) AS variacao_posicao, SIGN(flcr) * SIGN(captacao) < 0 AS muda_de_sinal
  FROM (SELECT nome_unidade, SUM(vl_flcr) flcr, SUM(vl_captacao_liquida) captacao FROM grupo_periodo
        WHERE periodo_ano = '2025' GROUP BY nome_unidade HAVING SUM(vl_contribuicao) > 0))
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação Q5:** *(preencher)*
# MAGIC
# MAGIC ## Discussão geral
# MAGIC *(preencher após a execução: conectar Q1–Q5 ao problema central; limitações — valores nominais, VGBL sem UF, direção R/D baseada em fonte secundária, conciliação PGBL parcial)*
