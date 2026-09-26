# Fluxos da previdência complementar aberta no Brasil (2014–2026)
### Contribuições, resgates e portabilidade de VGBL e PGBL por grupo econômico — MVP de Engenharia de Dados (PUC-Rio)

Pipeline de dados construído no **Databricks Free Edition** (Unity Catalog + Delta Lake + PySpark/Spark SQL), organizado na arquitetura medalhão **Bronze → Silver → Gold**, a partir da base pública do **SES — Sistema de Estatísticas da SUSEP**.

| Notebook | Conteúdo |
|---|---|
| [`notebooks/01_bronze.py`](notebooks/01_bronze.py) | Ingestão dos 9 CSVs do Volume para tabelas Delta, sem transformação, com metadados de rastreabilidade |
| [`notebooks/02_silver_qualidade.py`](notebooks/02_silver_qualidade.py) | Diagnóstico e tratamento dos problemas de qualidade; tabelas Silver; quarentena; testes |
| [`notebooks/03_gold_modelo_catalogo.py`](notebooks/03_gold_modelo_catalogo.py) | Esquema estrela, PK/FK, testes da Gold e catálogo de dados |
| [`notebooks/04_analises.py`](notebooks/04_analises.py) | Consultas e visualizações que respondem às perguntas de negócio |

Execução: sequencial, 01 → 02 → 03 → 04, no Databricks (catálogo `previdencia`, schemas `bronze`, `silver`, `gold`).

---

## 1. Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

### Contexto
A previdência complementar aberta reúne planos oferecidos por seguradoras e entidades abertas de previdência (EAPP), supervisionadas pela SUSEP. Os dois produtos dominantes são o **VGBL** (seguro de pessoas com cobertura por sobrevivência) e o **PGBL** (plano de previdência). Juntos, eles acumulam da ordem de R$ 1,8 trilhão em provisões e movimentam centenas de bilhões de reais por ano em contribuições, resgates e portabilidades.

A SUSEP publica esses dados no SES, a partir dos Formulários de Informações Periódicas (FIP) enviados pelas empresas. Os dados são públicos, mas chegam fragmentados em dezenas de arquivos, com:
- encoding Windows-1252;
- vírgula decimal;
- chaves com espaços;
- mudança de granularidade ao longo do tempo;
- duplicatas;
- domínios não documentados;
- grupo econômico ausente do cadastro;
- divergências entre tabelas.

### Problema
Não existe uma base integrada e auditável que permita acompanhar **entradas e saídas de recursos de VGBL e PGBL** e **distribuí-las entre grupos econômicos** com métricas definidas explicitamente. Comparar os arquivos do SES diretamente leva a erros de soma (sub-linhas e duplicatas), de atribuição (grupo econômico muda no tempo) e de interpretação (portabilidade misturada a contribuição).

### Objetivo geral
Construir, no Databricks, um pipeline Bronze → Silver → Gold reprodutível sobre o SES/SUSEP que responda às perguntas abaixo.

### Objetivos específicos
1. Ingerir os arquivos preservando o original, com metadados de rastreabilidade.
2. Diagnosticar e tratar, de forma documentada, os problemas de qualidade.
3. Integrar contribuições, resgates, portabilidade, provisões e grupo econômico com atribuição temporal.
4. Modelar um esquema estrela com catálogo de dados.
5. Responder às perguntas com Spark SQL.

### Perguntas de negócio
| # | Pergunta |
|---|---|
| Q1 | Como evoluíram contribuições, resgates e FLCR de VGBL e PGBL de 2014 a 2025, e como jan–jul/2026 se compara a jan–jul dos anos anteriores? |
| Q2 | Como evoluiu a taxa de resgate (resgates ÷ PMBaC média) de VGBL e PGBL? |
| Q3 | Qual o grau de concentração das contribuições entre grupos econômicos (participação dos 5 maiores e HHI), por produto e ano? |
| Q4 | Qual o saldo líquido de portabilidade por grupo econômico e qual a sua magnitude relativa às contribuições do grupo? |
| Q5 | Em que medida o resultado e a posição dos grupos mudam quando se passa do FLCR para a captação líquida (FLCR + saldo líquido de portabilidade)? |

### Definições das métricas
| Métrica | Fórmula | Origem | Grão | Limitações |
|---|---|---|---|---|
| Contribuições | Σ `contrib` (`tipoProd` ∈ {VGBL, PGBL}) | `Ses_Contrib_Benef` | empresa × mês × produto | Valores nominais |
| Resgates | Σ (`resg_total` + `resg_parcial`) após remover duplicatas exatas e somar as sub-linhas | `Ses_vgbl_resgates`, `ses_pgbl_resgates` | empresa × mês × produto | Sub-linhas pós-12/2013 não identificadas; `Resg_Pag_programado` (não documentado) fora da soma |
| **FLCR** (fluxo líquido de contribuições e resgates) | Contribuições − Resgates | acima | agregável | Não inclui portabilidade, benefícios, rentabilidade; não é variação da reserva |
| **Saldo líquido de portabilidade** | Aceita (`TIPOTRANSF`=R) − Cedida (`TIPOTRANSF`=D) | `ses_transferenciasexternas` (`TIPOPLANO` ∈ {VGBL, PGBL}) | empresa/grupo × produto × período | Direção R/D confirmada no SES online; tende a ~0 no agregado do mercado |
| **Captação líquida** | FLCR + Saldo líquido de portabilidade | acima | empresa/grupo × período | Conceito de mercado (relatórios baseados no SES), não definição normativa; exclui benefícios |
| **Taxa de resgate** | Σ resgates do período ÷ média dos saldos mensais de PMBaC | resgates + `Ses_vgbl_fundos`, `ses_pgbl_fundos` | produto × ano | Denominador afetado por rentabilidade e portabilidade; 2026 só jan–jul |
| **HHI** | Σ (participação × 100)² das contribuições | contribuições + grupo | produto × ano | Empresas do grupo 99999 ("outros") ou sem grupo contam como unidades próprias |

**Período:** jan/2014 a jul/2026. 2014 é o primeiro ano completo após a mudança de granularidade de 12/2013. Anos completos: 2014–2025. **2026 só é comparado em base jan–jul.**

### Dados brutos, estrutura e licença
**Fonte:** SUSEP — SES, *Base de Dados do SES* (`BaseCompleta.zip`), gerada em 21/09/2026, dados até 07/2026 — https://www2.susep.gov.br/menuestatistica/ses/principal.aspx. Documentação oficial das tabelas: `Documentacao_das_tabelas.rtf` (mesma página).

**Licença:** a página do SES informa que o sistema tem "o objetivo de fornecer ao público em geral estatísticas dos mercados supervisionados pela SUSEP". O download não traz um arquivo de licença específico. Os dados são publicados por órgão federal no âmbito da Política de Dados Abertos do Poder Executivo federal (Decreto nº 8.777/2016). O uso neste trabalho é acadêmico, não comercial, com citação da fonte. Os dados **não** são redistribuídos neste repositório.

| Arquivo | Linhas* | Colunas (originais) | Conteúdo |
|---|---|---|---|
| `Ses_Contrib_Benef.csv` | 28.185 | coenti, damesano, tipoProd, contrib, benef | Contribuições e benefícios por produto |
| `Ses_vgbl_resgates.csv` | 19.682 | damesano, coenti, resg_total, resg_parcial, Resg_Pag_programado | Resgates VGBL |
| `ses_pgbl_resgates.csv` | 18.016 | idem | Resgates PGBL |
| `ses_transferenciasexternas.csv` | 17.746 | COENTI, DAMESANO, TIPOTRANSF, TIPOPLANO, VALOR, QUANTIDADE | Portabilidades externas |
| `Ses_vgbl_fundos.csv` | 6.184 | coenti, damesano, fundos | PMBaC VGBL (fundos) |
| `ses_pgbl_fundos.csv` | 7.122 | coenti, damesano, fundos | PMBaC PGBL (fundos) |
| `ses_pgbl_uf.csv` | 129.216 | COENTI, DAMESANO, UF, CONTRIB, BENEFPAGO, RESGPAGO, NUMPARTIC, NUMBENEF, NUMRESG | PGBL por UF (somente conciliação) |
| `Ses_cias.csv` | 769 | Coenti, Noenti, Cogrupo, Nogrupo | Cadastro de empresas |
| `Ses_grupos_economicos.csv` | 66.461 | damesano, coenti, noenti, cogrupo, nogrupo | Grupo econômico por empresa e mês |

\* confirmar com a tabela de evidências do notebook 01.

---

## 2. Carga dos Dados (Etapa 4.2)

1. Download manual do `BaseCompleta.zip` na página do SES, em 26/09/2026, e descompactação local. O ZIP contém 40 CSVs. **9 foram selecionados** para o escopo.
2. Upload dos 9 CSVs para o **Volume do Unity Catalog** `/Volumes/previdencia/bronze/raw/ses/` pela interface do Databricks. O Free Edition restringe o acesso de saída à internet, e o upload para Volume é o caminho documentado pela plataforma.
3. O notebook [`01_bronze.py`](notebooks/01_bronze.py) lê cada CSV com `sep=';'`, `encoding=windows-1252` e todas as colunas como texto. Ele grava tabelas Delta em `previdencia.bronze.*` e acrescenta os metadados `_arquivo_origem`, `_data_geracao_base_ses`, `_mes_referencia_max`, `_ts_ingestao` e `_hash_linha`.
4. Teste de completude da carga: nº de linhas de cada tabela = nº de linhas de dados do arquivo.

📷 *Evidências:* `docs/img/01_volume_raw.png`, `docs/img/01_bronze_contagens.png`, `docs/img/01_bronze_schema.png`

---

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

### Modelo — esquema estrela
```
                dim_tempo (mes_ref)
                       │ 1:N
 dim_empresa ──1:N── fato_fluxo_previdencia ──N:1── dim_produto
 (cod_empresa)   (mes_ref, cod_empresa, cod_produto)   (cod_produto)
                       │ N:1
                  dim_grupo (cod_grupo)   ← grupo vigente no mês (ponte_empresa_grupo_mes)

 fato_pmbac (mes_ref, cod_empresa, cod_produto) → mesmas dimensões
```
| Tabela | Tipo | Grão | PK | FKs |
|---|---|---|---|---|
| `fato_fluxo_previdencia` | fato transacional (fluxos) | mês × empresa × produto | mes_ref, cod_empresa, cod_produto | tempo, empresa, produto, grupo |
| `fato_pmbac` | fato *snapshot* (estoque) | mês × empresa × produto | idem | idem |
| `ponte_empresa_grupo_mes` | associação temporal | empresa × mês | cod_empresa, mes_ref | empresa, grupo, tempo |
| `dim_tempo` | dimensão | mês | mes_ref | — |
| `dim_produto` | dimensão | produto | cod_produto | — |
| `dim_empresa` | dimensão | empresa | cod_empresa | — |
| `dim_grupo` | dimensão | grupo econômico | cod_grupo | — |

**Por que um único fato de fluxo:** contribuições, resgates e portabilidade têm exatamente o mesmo grão, e as métricas derivadas (FLCR, captação líquida) combinam as três. **Por que a ponte temporal:** 74 das 124 empresas de previdência mudaram de grupo econômico ao longo do histórico. Atribuir tudo ao grupo atual distorceria concentração e rankings. Cada fato recebe o grupo **vigente no mês** (EXATA) ou o último conhecido (HERDADA).

### Catálogo de dados
O catálogo está (i) gravado como `COMMENT` em cada tabela e coluna no **Unity Catalog** e (ii) consolidado na tabela `gold.catalogo_dados`, com descrição, tipo, domínio observado (mínimo/máximo ou categorias), % de nulos e linhagem.

*(transcrever aqui a tabela do catálogo gerada no notebook 03)*

📷 *Evidências:* `docs/img/03_catalog_explorer.png`, `docs/img/03_catalogo_dados.png`, `docs/img/03_constraints.png`

---

## 4. Pipeline de Dados (Etapa 4.4)

O pipeline foi organizado em quatro notebooks executados em sequência. Cada um lê apenas tabelas da camada anterior:

| Etapa | Entrada | Saída | Principais transformações |
|---|---|---|---|
| 01 Bronze | CSVs no Volume | `bronze.*` (9 tabelas) | nenhuma no conteúdo; metadados de ingestão |
| 02 Silver | `bronze.*` | `silver.empresa`, `empresa_grupo_mensal`, `contribuicoes`, `resgates`, `portabilidade`, `pmbac`, `dq_conciliacao_pgbl`, `dq_resultados`, `quarentena` | trim, padronização de nomes, vírgula→ponto, DECIMAL(20,2), AAAAMM→DATE, remoção de duplicatas exatas, agregação de sub-linhas, mapeamento de domínios, flags, quarentena |
| 03 Gold | `silver.*` | dimensões, ponte, fatos, `catalogo_dados` | *as-of join* de grupo, *full outer* das métricas, cálculo de FLCR e captação líquida, PK/FK, testes de conservação |
| 04 Análises | `gold.*` | consultas e gráficos | agregações, HHI, rankings |

📷 *Evidências:* `docs/img/02_silver_tabelas.png`, `docs/img/03_gold_tabelas.png`, `docs/img/03_testes_gold.png`

---

## 5. Qualidade de Dados (Etapa 4.5)

Todos os testes ficam em `silver.dq_resultados`. Os registros não interpretáveis ficam em `silver.quarentena`.

| Problema | Evidência | Tratamento | Justificativa |
|---|---|---|---|
| Encoding Windows-1252 | nomes corrompidos se lidos como UTF-8 | leitura com `windows-1252` | charset real do arquivo |
| Vírgula decimal / resíduo float | `350407,58`; `31557844,9700002` | vírgula→ponto; `DECIMAL(20,2)` | tipagem correta |
| Período AAAAMM em texto | `damesano` | → `DATE`; inválidos em quarentena | operações temporais |
| Nomes de colunas inconsistentes | `coenti` / `COENTI` / `Coenti` | *snake_case* padronizado | joins e catálogo |
| Espaços nas chaves | 100% das chaves de `Ses_cias` e dos arquivos de fundos | `trim` | sem isso o join falha |
| Colunas 100% vazias | `Cogrupo`/`Nogrupo` (cias); `BENEFPAGO`/`NUMBENEF` (pgbl_uf) | excluídas / não usadas | sem conteúdo |
| Coluna não documentada | `Resg_Pag_programado` | mantida em coluna própria, fora de `vl_resgate` | transparência |
| Duplicatas exatas (resgates) | *(valor do notebook)* linhas, **soma = 0** | removidas após provar soma zero | não carregam informação |
| Mudança de granularidade | 1 linha por empresa×mês até 11/2013; várias a partir de 12/2013 | soma por empresa×mês×produto | parcelas do mesmo total; teste de continuidade |
| Domínio de `TIPOTRANSF` | D, R, **r**, **P** | R→aceita, D→cedida; r e P em quarentena | documentação só diz "Aceita ou Cedida" |
| Quantidade implausível | 2,75 bilhões de portabilidades em um mês | flag; quantidade não usada | acurácia |
| Contribuições negativas | *(valor do notebook)* | mantidas com flag | estornos legítimos |
| Grupo econômico no tempo | 74 de 124 empresas mudaram de grupo; meses sem grupo | ponte *as-of*; tipo de atribuição registrado | atribuição correta |
| Integridade de empresas | fatos × cadastro | teste de órfãos | integridade referencial |
| Conciliação PGBL entre fontes | *(% conciliado)* | nenhuma correção; indicador publicado | fontes de quadros diferentes do FIP |

**Direção da portabilidade (R/D).** A documentação das tabelas informa apenas "Tipo de transferência (Aceita ou Cedida)", sem dizer qual letra corresponde a cada direção. O mapeamento foi **confirmado na consulta oficial do SES online** (*Previdência: Portabilidades Externas*, empresa 04031, grupo VGBL, período 202509):

| SES online | Valor | Quantidade | Linha no CSV |
|---|---|---|---|
| Valor Aceito | R$ 2.757.725.564 | 12.805 | `04031;202509;R;VGBL;2757725563,85;12805` |
| Valor Cedido | R$ 287.146.012 | 1.140 | `04031;202509;D;VGBL;287146011,54;1140` |

Portanto **R = aceita** e **D = cedida**. O resultado coincide com a metodologia de um relatório de mercado baseado no SES (Caixa Seguridade) e com a simetria dos totais anuais de mercado. Os códigos `r` e `P`, que não aparecem na documentação, **não** foram interpretados por analogia e ficaram em quarentena.

📷 *Evidência:* `docs/img/02_ses_online_portabilidade.png`

📷 *Evidências:* `docs/img/02_dq_resultados.png`, `docs/img/02_granularidade.png`, `docs/img/02_conciliacao.png`, `docs/img/02_quarentena.png`

---

## 6. Análise de Dados (Etapa 4.5)

*(preencher com resultados, gráficos e interpretação de Q1–Q5 após a execução)*

### Discussão geral
*(preencher)*

---

## 7. Autoavaliação

*(preencher: objetivos atingidos; dificuldades — granularidade oculta, domínio não documentado, restrição de internet no Free Edition, prazo; limitações — valores nominais, VGBL sem UF, direção R/D por fonte secundária, conciliação PGBL parcial, sem inferência causal)*

### Trabalhos futuros
- Correção monetária pelo IPCA (BCB/SGS 433).
- VGBL por UF via `SES_UF2.csv` (ramos 0994/1392) e análise per capita com população do IBGE.
- Previdência tradicional (`ses_prev_trad_resgates.csv`).
- Quantidade de participantes (`ses_quantprev_part.csv`), após resolver estoques negativos e sub-linhas.
- Orquestração como *Job* do Databricks com ingestão incremental de novas bases do SES (os *snapshots* já são identificados por `_data_geracao_base_ses`).
- Análise de eventos regulatórios (ex.: IOF sobre VGBL em 2025) com metodologia causal apropriada.

---
*Autor: Danilo Leandro Gomes dos Santos · Pós-graduação em Ciência de Dados e Analytics — PUC-Rio · Disciplina de Engenharia de Dados*
