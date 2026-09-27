<div align="center">

# Fluxos da Previdência Complementar Aberta no Brasil (2014–2026)

### Contribuições, resgates e portabilidade de VGBL e PGBL por grupo econômico

**MVP — Construção de um Pipeline de Dados na Nuvem**

Disciplina: Engenharia de Dados  
Pós-Graduação em Ciência de Dados e Analytics  
Pontifícia Universidade Católica do Rio de Janeiro (PUC-Rio)

**Aluno(a):** Danilo Leandro Gomes dos Santos

Setembro de 2026

---

*Plataforma: Databricks Free Edition (Unity Catalog · Delta Lake · PySpark / Spark SQL)*  
*Fonte de dados: SUSEP — Sistema de Estatísticas da SUSEP (SES)*

</div>

---

## Glossário de siglas e termos

**Mercado de previdência**

| Sigla / termo | Significado |
|---|---|
| **SUSEP** | Superintendência de Seguros Privados — autarquia federal que regula e fiscaliza seguros, previdência complementar aberta e capitalização |
| **SES** | Sistema de Estatísticas da SUSEP — base pública de onde vêm todos os dados deste trabalho |
| **FIP** | Formulário de Informações Periódicas — relatório que as empresas supervisionadas enviam à SUSEP; é a origem dos dados do SES |
| **EAPP** | Entidade Aberta de Previdência Complementar — empresa autorizada a vender planos de previdência abertos ao público |
| **Previdência complementar aberta** | Planos de previdência privada vendidos ao público por seguradoras e EAPPs (diferente dos fundos de pensão fechados, ligados a uma empresa ou categoria) |
| **PGBL** | Plano Gerador de Benefício Livre — plano de previdência cujas contribuições podem ser deduzidas da base do IR (até 12% da renda bruta, na declaração completa); o IR incide sobre o valor total resgatado |
| **VGBL** | Vida Gerador de Benefício Livre — seguro de pessoas com cobertura por sobrevivência; não há dedução no IR, e o imposto incide apenas sobre os rendimentos |
| **PAGP, PRGP, VAGP, VRGP** | Modalidades menos comuns de planos (P = previdência, V = vida) com atualização ou remuneração garantida; aparecem nos dados de portabilidade, mas ficaram fora do escopo |
| **Previdência tradicional** | Planos antigos com rentabilidade mínima garantida; fora do escopo deste MVP |
| **Contribuição** | Valor aportado pelo participante no plano (entrada de recursos) |
| **Resgate** | Retirada de recursos pelo participante antes ou em vez de receber um benefício (saída de recursos); pode ser total ou parcial |
| **Benefício** | Pagamento de renda ao participante na fase de recebimento (aposentadoria, pensão) |
| **Portabilidade** | Transferência da reserva acumulada de um plano para outro, geralmente entre empresas diferentes, sem resgate e sem incidência de IR. **Aceita** = recebida pela empresa; **cedida** = enviada |
| **PMBaC** | Provisão Matemática de Benefícios a Conceder — reserva acumulada pelos participantes que ainda não recebem benefício; é o "estoque" de recursos do plano |
| **Grupo econômico** | Conjunto de empresas sob o mesmo controle (ex.: todas as seguradoras de um mesmo banco) |
| **Código 99999** | Código genérico usado pela SUSEP para empresas classificadas como "OUTROS GRUPOS"; não representa um grupo real |
| **IOF** | Imposto sobre Operações Financeiras — citado no contexto das mudanças tributárias sobre aportes em VGBL em 2025 |

**Métricas definidas neste trabalho**

| Sigla / termo | Significado |
|---|---|
| **FLCR** | Fluxo Líquido de Contribuições e Resgates = contribuições − resgates |
| **Saldo líquido de portabilidade** | Portabilidade aceita − portabilidade cedida |
| **Captação líquida** | FLCR + saldo líquido de portabilidade (conceito usado pelo mercado) |
| **Taxa de resgate** | Resgates do período ÷ PMBaC média do período |
| **HHI** | Índice Herfindahl-Hirschman — medida de concentração de mercado: soma dos quadrados das participações (em %) de cada unidade; vai de 0 (pulverizado) a 10.000 (monopólio) |
| **Top-5** | Participação somada dos 5 maiores grupos no mercado |
| **Unidade econômica** | Grupo econômico vigente no mês ou, para empresas do código 99999, a própria empresa |
| **YTD / jan–jul** | *Year to date* — acumulado de janeiro a julho, usado para comparar 2026 (ano incompleto) com os anos anteriores |

**Termos técnicos**

| Sigla / termo | Significado |
|---|---|
| **AAAAMM** | Formato de data usado no SES: ano com 4 dígitos + mês com 2 dígitos (ex.: 202507 = julho de 2025) |
| **Bronze / Silver / Gold** | Camadas da arquitetura medalhão: dado bruto → dado limpo e padronizado → dado modelado para análise |
| **Delta Lake** | Formato de tabela usado pelo Databricks, com transações ACID e histórico de versões |
| **Unity Catalog** | Catálogo de dados do Databricks: organiza tabelas, comentários, permissões e linhagem |
| **PK / FK** | *Primary Key* (chave primária) / *Foreign Key* (chave estrangeira) |
| **Linhagem (*lineage*)** | Rastro de origem de cada tabela: de quais tabelas ela foi gerada |
| **Quarentena** | Tabela onde ficam os registros que não puderam ser interpretados com segurança, preservados sem descarte |

---


## Visão geral do repositório

Pipeline de dados construído no **Databricks Free Edition** (Unity Catalog, Delta Lake, PySpark e Spark SQL), organizado na arquitetura medalhão Bronze → Silver → Gold a partir da base pública do SES/SUSEP.

| Notebook | Conteúdo |
|---|---|
| [`notebooks/01_bronze.py`](notebooks/01_bronze.py) | Ingestão dos 9 CSVs do Volume para tabelas Delta, sem transformação, com metadados de rastreabilidade |
| [`notebooks/02_silver_qualidade.py`](notebooks/02_silver_qualidade.py) | Diagnóstico e tratamento dos problemas de qualidade; tabelas Silver; quarentena; testes |
| [`notebooks/03_gold_modelo_catalogo.py`](notebooks/03_gold_modelo_catalogo.py) | Esquema estrela, PK/FK, testes da Gold e catálogo de dados |
| [`notebooks/04_analises.py`](notebooks/04_analises.py) | Consultas e visualizações que respondem às perguntas de negócio |

Execução sequencial (01 → 02 → 03 → 04) no Databricks, no catálogo `previdencia`, com os schemas `bronze`, `silver` e `gold`.

---

## 1. Contexto de Negócios e Perguntas

### Contexto
A previdência complementar aberta reúne planos oferecidos por seguradoras e entidades abertas de previdência (EAPP), supervisionadas pela SUSEP. Os dois principais produtos são o VGBL e o PGBL, que somavam cerca de R$ 1,8 trilhão em PMBaC em julho de 2026 e movimentam centenas de bilhões de reais por ano em contribuições, resgates e portabilidades.

A SUSEP publica esses dados no SES, a partir dos Formulários de Informações Periódicas (FIP) enviados pelas empresas. Os dados são públicos, mas estão distribuídos em dezenas de arquivos e apresentam problemas de formato, granularidade e documentação que precisam ser tratados antes de qualquer análise (detalhados na seção 5).

### Problema
Os dados do SES não estão integrados de forma que permita acompanhar entradas e saídas de VGBL e PGBL por grupo econômico. O uso direto dos arquivos pode gerar erros de soma, atribuição incorreta de grupos ao longo do tempo e interpretação equivocada de fluxos como contribuições, resgates e portabilidades.

### Objetivo geral
Construir, no Databricks, um pipeline Bronze → Silver → Gold reprodutível sobre o SES/SUSEP que responda às perguntas de negócio abaixo.

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
| Q4 | Qual o saldo líquido de portabilidade por grupo econômico (unidade econômica) e qual a sua magnitude relativa às contribuições? |
| Q5 | Em que medida o resultado e a posição dos grupos mudam quando se passa do FLCR para a captação líquida (FLCR + saldo líquido de portabilidade)? |

### Definições das métricas
| Métrica | Fórmula | Origem | Grão (granularidade) | Limitações |
|---|---|---|---|---|
| Contribuições | Σ `contrib` (`tipoProd` ∈ {VGBL, PGBL}) | `Ses_Contrib_Benef` | empresa × mês × produto | Valores nominais |
| Resgates | Σ (`resg_total` + `resg_parcial`) após remover duplicatas exatas e somar as sub-linhas | `Ses_vgbl_resgates`, `ses_pgbl_resgates` | empresa × mês × produto | Sub-linhas pós-12/2013 não identificadas; `Resg_Pag_programado` (não documentado) fora da soma |
| **FLCR** (fluxo líquido de contribuições e resgates) | Contribuições − Resgates | `Ses_Contrib_Benef`; `Ses_vgbl_resgates`, `ses_pgbl_resgates` | agregável | Não inclui portabilidade, benefícios nem rentabilidade; não é a variação da reserva |
| **Saldo líquido de portabilidade** | Aceita (`TIPOTRANSF`=R) − Cedida (`TIPOTRANSF`=D) | `ses_transferenciasexternas` (`TIPOPLANO` ∈ {VGBL, PGBL}) | empresa/grupo × produto × período | Direção R/D confirmada no SES online; tende a ~0 no agregado do mercado |
| **Captação líquida** | FLCR + Saldo líquido de portabilidade | `Ses_Contrib_Benef`; `Ses_vgbl_resgates`, `ses_pgbl_resgates`; `ses_transferenciasexternas` | empresa/grupo × período | Conceito usado em relatórios de mercado baseados no SES (ex.: Caixa Seguridade), não é definição normativa; exclui benefícios |
| **Taxa de resgate** | Σ resgates do período ÷ média dos saldos mensais de PMBaC | `Ses_vgbl_resgates`, `ses_pgbl_resgates`; `Ses_vgbl_fundos`, `ses_pgbl_fundos` | produto × ano | Denominador afetado por rentabilidade e portabilidade; 2026 só jan–jul |
| **HHI** | Σ (participação × 100)² das contribuições | `Ses_Contrib_Benef`; `Ses_grupos_economicos`; `Ses_cias` | produto × ano | Empresas do grupo 99999 ou sem grupo contam como unidades próprias |

**Unidade econômica (Q3, Q4, Q5):** grupo econômico vigente no mês. Quando a empresa está no código genérico 99999 ("OUTROS GRUPOS"), que não é um grupo real, a própria empresa é a unidade, identificada como "(sem grupo)".

**Período:** jan/2014 a jul/2026. 2014 é o primeiro ano completo após a mudança de granularidade de 12/2013. As comparações anuais usam 2014–2025, e 2026 só é comparado em base jan–jul.

### Dados brutos, estrutura e licença
**Fonte:** SUSEP — SES, *Base de Dados do SES* (`BaseCompleta.zip`), gerada em 21/09/2026, com dados até 07/2026: https://www2.susep.gov.br/menuestatistica/ses/principal.aspx. A documentação oficial das tabelas (`Documentacao_das_tabelas.rtf`) está na mesma página.

**Licença:** não foi identificado arquivo de licença específico junto ao download. A base é disponibilizada publicamente pela SUSEP para fornecimento de estatísticas dos mercados supervisionados. Neste projeto, os dados são utilizados para finalidade acadêmica, com indicação da fonte, e os arquivos brutos não são redistribuídos no repositório.

| Arquivo | Linhas* | Colunas (originais) | Conteúdo |
|---|---|---|---|
| `Ses_Contrib_Benef.csv` | 28.185 | coenti, damesano, tipoProd, contrib, benef | Contribuições e benefícios por produto |
| `Ses_vgbl_resgates.csv` | 19.682 | damesano, coenti, resg_total, resg_parcial, Resg_Pag_programado | Resgates VGBL |
| `ses_pgbl_resgates.csv` | 18.016 | damesano, coenti, resg_total, resg_parcial, Resg_Pag_programado | Resgates PGBL |
| `ses_transferenciasexternas.csv` | 17.746 | COENTI, DAMESANO, TIPOTRANSF, TIPOPLANO, VALOR, QUANTIDADE | Portabilidades externas |
| `Ses_vgbl_fundos.csv` | 6.184 | coenti, damesano, fundos | PMBaC VGBL (fundos) |
| `ses_pgbl_fundos.csv` | 7.122 | coenti, damesano, fundos | PMBaC PGBL (fundos) |
| `ses_pgbl_uf.csv` | 129.216 | COENTI, DAMESANO, UF, CONTRIB, BENEFPAGO, RESGPAGO, NUMPARTIC, NUMBENEF, NUMRESG | PGBL por UF (usado apenas na conciliação) |
| `Ses_cias.csv` | 769 | Coenti, Noenti, Cogrupo, Nogrupo | Cadastro de empresas |
| `Ses_grupos_economicos.csv` | 66.461 | damesano, coenti, noenti, cogrupo, nogrupo | Grupo econômico por empresa e mês |

\* Contagens confirmadas no notebook 01 (linhas no arquivo = linhas na tabela Bronze nos 9 arquivos).

---

## 2. Carga dos Dados

1. Download manual do `BaseCompleta.zip` na página do SES, em 26/09/2026, e descompactação local. O ZIP contém 40 CSVs, dos quais 9 foram selecionados para o escopo.
2. Upload dos 9 CSVs para o Volume do Unity Catalog `/Volumes/previdencia/bronze/raw/ses/` pela interface do Databricks. O Free Edition restringe o acesso de saída à internet, e o upload para Volume é o caminho documentado pela plataforma.
3. O notebook [`01_bronze.py`](notebooks/01_bronze.py) lê cada CSV com `sep=';'`, `encoding=windows-1252` e todas as colunas como texto, grava tabelas Delta em `previdencia.bronze.*` e acrescenta os metadados `_arquivo_origem`, `_data_geracao_base_ses`, `_mes_referencia_max`, `_ts_ingestao` e `_hash_linha`.
4. Teste de completude da carga: o número de linhas de cada tabela Bronze deve ser igual ao número de linhas de dados do arquivo.

**Volume com os arquivos brutos**
![Volume raw](docs/img/01_volume_raw.png)

**Contagem de linhas: arquivo × tabela Bronze (9/9 OK)**
![Contagens Bronze](docs/img/01_bronze_contagens.png)

**Schema de uma tabela Bronze (colunas de negócio como string + metadados; formato Delta)**
![Schema Bronze](docs/img/01_bronze_schema.png)

---

## 3. Modelagem e Catálogo de Dados

### 3.1 Modelo: esquema estrela com associação temporal de grupo
```
                          dim_tempo (mes_ref)
                                 │ 1:N
 dim_empresa ──1:N──►  fato_fluxo_previdencia  ◄──N:1── dim_produto
 (cod_empresa)        (mes_ref, cod_empresa, cod_produto)   (cod_produto)
       │                         │ N:1
       │ 1:N                dim_grupo (cod_grupo)
       ▼                         ▲ N:1
 ponte_empresa_grupo_mes ────────┘   (grupo vigente para cada empresa em cada mês)

 fato_pmbac (mes_ref, cod_empresa, cod_produto) → mesmas dimensões (tempo, empresa, produto, grupo)
```

| Tabela | Tipo | Grão | PK | FKs | Linhas |
|---|---|---|---|---|---|
| `fato_fluxo_previdencia` | fato de fluxos (mensal) | mês × empresa × produto | mes_ref, cod_empresa, cod_produto | tempo, empresa, produto, grupo | 6.664 |
| `fato_pmbac` | fato *snapshot* (estoque de fim de mês) | mês × empresa × produto | mes_ref, cod_empresa, cod_produto | tempo, empresa, produto, grupo | — |
| `ponte_empresa_grupo_mes` | associação temporal | empresa × mês | cod_empresa, mes_ref | empresa, grupo, tempo | 3.732 |
| `dim_tempo` | dimensão | mês (jan/2014–jul/2026) | mes_ref | — | 151 |
| `dim_produto` | dimensão | produto | cod_produto | — | 2 |
| `dim_empresa` | dimensão | empresa | cod_empresa | — | 769 |
| `dim_grupo` | dimensão | grupo econômico | cod_grupo | — | 122 |

**Decisões de modelagem**
- **Um único fato de fluxo.** Contribuições, resgates e portabilidade têm o mesmo grão (mês × empresa × produto), e as métricas derivadas combinam as três (FLCR e captação líquida). Por isso ficaram em uma só tabela. As chaves das três origens são unidas e cada origem é associada a esse conjunto de chaves, de modo que um mês com resgate ou portabilidade, mas sem contribuição, continua presente. Valores ausentes viram 0, e as *flags* `fl_tem_contribuicao`, `fl_tem_resgate` e `fl_tem_portabilidade` indicam de onde veio cada valor.
- **Fato de estoque separado.** A PMBaC é uma posição de fim de mês e não pode ser somada ao longo do tempo. Por isso fica em `fato_pmbac`, com o mesmo grão e as mesmas dimensões.
- **Grupo econômico com atribuição temporal.** 175 empresas do mercado (74 das 124 de previdência) mudaram de grupo ao longo do histórico. Por exemplo, a empresa 05843 passou por Liberty, Talanx, Indiana, Bradesco e Independente, e as empresas 06238, 05665 e 06181 passaram por Vera Cruz, MAPFRE e BBMAPFRE. Usar o grupo atual para todo o histórico distorceria a concentração e os rankings. A `ponte_empresa_grupo_mes` define, para cada empresa × mês, o grupo vigente naquele mês (*as-of join*: o último grupo informado com mês ≤ mês do fato), e esse `cod_grupo` é gravado nas tabelas fato. Resultado: 3.731 atribuições EXATAS, 1 HERDADA (1 mês de defasagem) e 0 NAO_INFORMADO. As análises Q3–Q5 usam essa atribuição.
- **Chaves no Unity Catalog.** Foram criadas 7 PKs e 11 FKs como *constraints* informativas. A validade delas é verificada pelos testes da Gold (unicidade e órfãos = 0).

![Atribuição temporal de grupo](docs/img/03_ponte_atribuicao.png)
![Histórico de grupos por empresa](docs/img/02_grupos_temporais.png)

### 3.2 Catálogo de dados
O catálogo foi construído de três formas complementares:
1. `COMMENT` em cada tabela e coluna das camadas Silver e Gold no Unity Catalog, visível no Catalog Explorer, com a descrição e a linhagem da tabela;
2. tabela `gold.catalogo_dados`, gerada no notebook 03, que registra para cada coluna da Silver e da Gold a descrição, o tipo, o domínio observado (calculado a partir dos dados: mínimo/máximo para números e datas, categorias para textos) e o % de nulos. A parte da Gold foi exportada em [`docs/evidencias/catalogo_dados_gold.csv`](docs/evidencias/catalogo_dados_gold.csv);
3. linhagem descrita no comentário de cada tabela e também capturada automaticamente pelo Unity Catalog (grafo abaixo).

**Catálogo transcrito: modelo Gold**

#### `gold.fato_fluxo_previdencia`
*Fato de fluxos mensais por empresa e produto: contribuições, resgates, FLCR, portabilidade e captação líquida.*  
**Linhagem:** as chaves (mes_ref, cod_empresa, cod_produto) são obtidas pela união (`UNION`) de `silver.contribuicoes`, `silver.resgates` e `silver.portabilidade`. Cada uma dessas tabelas é associada às chaves por `LEFT JOIN`, o que equivale a um *full outer join* das três origens; ausências viram 0 e são sinalizadas pelas *flags* `fl_tem_*`. Em seguida, `INNER JOIN` com `gold.ponte_empresa_grupo_mes` por (cod_empresa, mes_ref) para obter o grupo vigente no mês. Filtro de período jan/2014–jul/2026; FLCR, saldo de portabilidade e captação líquida são calculados nessa etapa.

| Coluna | Descrição | Tipo | Domínio observado | % nulos |
|---|---|---|---|---|
| `cod_empresa` | Código FIP da empresa na SUSEP (5 dígitos, texto). | string | 30 valores distintos (ex.: 01848, 02101, 02682 ...) | 0 |
| `cod_grupo` | Código do grupo econômico (99999 = 'OUTROS GRUPOS', genérico; NAO_INFORMADO = sem histórico). | string | 21 valores distintos (ex.: 00019, 00027, 00035 ...) | 0 |
| `cod_produto` | Produto: VGBL ou PGBL. | string | {PGBL, VGBL} | 0 |
| `fl_contribuicao_negativa` | Verdadeiro se a contribuição informada é negativa (estorno/ajuste). Mantida nos totais. | boolean | {false} | 0 |
| `fl_qtd_portab_suspeita` | Verdadeiro se alguma linha de origem tem valor médio por portabilidade < R$ 100 (QUANTIDADE implausível). | boolean | {false, true} | 0 |
| `fl_tem_contribuicao` | Verdadeiro se a chave existe em silver.contribuicoes. | boolean | {false, true} | 0 |
| `fl_tem_portabilidade` | Verdadeiro se a chave existe em silver.portabilidade. | boolean | {false, true} | 0 |
| `fl_tem_resgate` | Verdadeiro se a chave existe em silver.resgates. | boolean | {false, true} | 0 |
| `mes_ref` | Mês de referência (1º dia do mês), derivado de damesano (AAAAMM). | date | [2014-01-01 ; 2026-07-01] | 0 |
| `vl_captacao_liquida` | Captação líquida (conceito de mercado) = vl_flcr + vl_portab_liquida. | decimal(20,2) | [-4038109018.01 ; 4196870481.15] | 0 |
| `vl_contribuicao` | Contribuições no mês (R$ nominais). Origem: contrib. | decimal(20,2) | [0.00 ; 6277450353.05] | 0 |
| `vl_flcr` | FLCR — fluxo líquido de contribuições e resgates = vl_contribuicao − vl_resgate. | decimal(20,2) | [-1666920581.58 ; 4312297996.99] | 0 |
| `vl_portab_aceita` | Portabilidade aceita/recebida (TIPOTRANSF = R), R$ nominais. | decimal(20,2) | [-780180.94 ; 2757725563.85] | 0 |
| `vl_portab_cedida` | Portabilidade cedida/enviada (TIPOTRANSF = D), R$ nominais. | decimal(20,2) | [0.00 ; 2564267466.43] | 0 |
| `vl_portab_liquida` | Saldo líquido de portabilidade = aceita − cedida. | decimal(20,2) | [-2371188436.43 ; 2470579552.31] | 0 |
| `vl_resgate` | Resgates do projeto = resg_total + resg_parcial (R$ nominais). | decimal(20,2) | [0.00 ; 4527735624.30] | 0 |
| `vl_resgate_pag_programado` | Soma de Resg_Pag_programado (coluna não documentada pela SUSEP); fora de vl_resgate. | decimal(20,2) | [0.00 ; 1236136.78] | 0 |

#### `gold.fato_pmbac`
*Fato de estoque de PMBaC de fim de mês por empresa e produto.*  
**Linhagem:** `silver.pmbac` (união dos arquivos de fundos de VGBL e PGBL) com `INNER JOIN` em `gold.ponte_empresa_grupo_mes` por (cod_empresa, mes_ref) para obter o grupo vigente no mês. Filtro de período jan/2014–jul/2026.

| Coluna | Descrição | Tipo | Domínio observado | % nulos |
|---|---|---|---|---|
| `cod_empresa` | Código FIP da empresa na SUSEP (5 dígitos, texto). | string | 29 valores distintos (ex.: 01848, 02101, 02682 ...) | 0 |
| `cod_grupo` | Código do grupo econômico (99999 = 'OUTROS GRUPOS', genérico; NAO_INFORMADO = sem histórico). | string | 20 valores distintos (ex.: 00019, 00027, 00035 ...) | 0 |
| `cod_produto` | Produto: VGBL ou PGBL. | string | {PGBL, VGBL} | 0 |
| `mes_ref` | Mês de referência (1º dia do mês), derivado de damesano (AAAAMM). | date | [2014-01-01 ; 2026-07-01] | 0 |
| `vl_pmbac` | Provisão Matemática de Benefícios a Conceder aplicada em fundos, saldo de fim de mês (R$ nominais). | decimal(20,2) | [0.00 ; 434631324006.13] | 0 |

#### `gold.ponte_empresa_grupo_mes`
*Grupo vigente para cada empresa em cada mês (atribuição as-of).*  
**Linhagem:** chaves distintas (cod_empresa, mes_ref) obtidas da união de `silver.contribuicoes`, `silver.resgates`, `silver.portabilidade` e `silver.pmbac`, filtradas para jan/2014–jul/2026. Essas chaves são associadas a `silver.empresa_grupo_mensal` por `LEFT JOIN` em cod_empresa, com a condição mês do grupo ≤ mês da chave; um `ROW_NUMBER` ordenado pelo mês do grupo (decrescente) mantém apenas o registro mais recente (*as-of join*). Sem correspondência, o grupo recebe `NAO_INFORMADO`. `tipo_atribuicao` e `meses_defasagem` são derivados da diferença entre os dois meses.

| Coluna | Descrição | Tipo | Domínio observado | % nulos |
|---|---|---|---|---|
| `cod_empresa` | Código FIP da empresa na SUSEP (5 dígitos, texto). | string | 30 valores distintos (ex.: 01848, 02101, 02682 ...) | 0 |
| `cod_grupo` | Código do grupo econômico (99999 = 'OUTROS GRUPOS', genérico; NAO_INFORMADO = sem histórico). | string | 21 valores distintos (ex.: 00019, 00027, 00035 ...) | 0 |
| `mes_ref` | Mês de referência (1º dia do mês), derivado de damesano (AAAAMM). | date | [2014-01-01 ; 2026-07-01] | 0 |
| `meses_defasagem` | Meses entre o mês do fato e o mês do grupo utilizado (0 = exata). | int | [0 ; 1] | 0 |
| `tipo_atribuicao` | EXATA (grupo do próprio mês), HERDADA (último grupo anterior) ou NAO_INFORMADO. | string | {EXATA, HERDADA} | 0 |

#### `gold.dim_tempo`
*Dimensão de meses de jan/2014 a jul/2026.*  
**Linhagem:** não vem de tabela de origem. É gerada no notebook 03 com `sequence()` mensal de jan/2014 a jul/2026, e os atributos de ano, mês, trimestre, ano completo e período jan–jul são derivados de `mes_ref`.

| Coluna | Descrição | Tipo | Domínio observado | % nulos |
|---|---|---|---|---|
| `ano` | Ano civil. | int | [2014 ; 2026] | 0 |
| `ano_mes` | Mês de referência no formato original AAAAMM. | string | 151 valores distintos (ex.: 201401, 201402, 201403 ...) | 0 |
| `fl_ano_completo` | Verdadeiro para anos com 12 meses na base (2014–2025). | boolean | {false, true} | 0 |
| `fl_periodo_ytd` | Verdadeiro para meses jan–jul (base de comparação com 2026). | boolean | {false, true} | 0 |
| `mes` | Mês (1–12). | int | [1 ; 12] | 0 |
| `mes_ref` | Mês de referência (1º dia do mês). | date | [2014-01-01 ; 2026-07-01] | 0 |
| `trimestre` | Trimestre (1–4). | int | [1 ; 4] | 0 |

#### `gold.dim_produto`
*Dimensão de produtos do escopo (VGBL, PGBL).*  
**Linhagem:** não vem de tabela de origem. As duas linhas (VGBL e PGBL) são definidas diretamente no notebook 03, de acordo com o escopo do projeto.

| Coluna | Descrição | Tipo | Domínio observado | % nulos |
|---|---|---|---|---|
| `cod_produto` | Produto: VGBL ou PGBL. | string | {PGBL, VGBL} | 0 |
| `descricao_produto` | Descrição do produto. | string | 2 textos descritivos (um por produto) | 0 |
| `nome_produto` | Nome do produto. | string | {Plano Gerador de Benefício Livre, Vida Gerador de Benefício Livre} | 0 |

#### `gold.dim_empresa`
*Dimensão de empresas.*  
**Linhagem:** cópia de `silver.empresa`, sem joins. Essa tabela vem de `bronze.ses_cias`, com `trim` no código e no nome e exclusão das colunas `Cogrupo`/`Nogrupo` (100% vazias).

| Coluna | Descrição | Tipo | Domínio observado | % nulos |
|---|---|---|---|---|
| `cod_empresa` | Código FIP da empresa na SUSEP (5 dígitos, texto). | string | 769 valores distintos (ex.: 01007, 01015, 01058 ...) | 0 |
| `nome_empresa` | Razão social da empresa (Ses_cias). | string | 750 valores distintos (ex.: 180 SEGUROS S.A., 2P SEGUROS S.A., 88I SEGURADORA DIGITAL S… | 0 |

#### `gold.dim_grupo`
*Dimensão de grupos econômicos (nome mais recente) + NAO_INFORMADO.*  
**Linhagem:** `silver.empresa_grupo_mensal`, com um `ROW_NUMBER` por cod_grupo ordenado pelo mês (decrescente) para manter o nome mais recente de cada grupo, e `UNION ALL` de uma linha fixa `NAO_INFORMADO`.

| Coluna | Descrição | Tipo | Domínio observado | % nulos |
|---|---|---|---|---|
| `cod_grupo` | Código do grupo econômico (99999 = 'OUTROS GRUPOS', genérico; NAO_INFORMADO = sem histórico). | string | 122 valores distintos (ex.: 00019, 00027, 00035 ...) | 0 |
| `fl_grupo_generico` | Verdadeiro para o código 99999 e para NAO_INFORMADO (não são grupos reais; nas análises Q3–Q5 cada empresa conta como unidade própria). | boolean | {false, true} | 0 |
| `nome_grupo` | Nome do grupo econômico. | string | 122 valores distintos (ex.: ACE, AGF BRASIL, ALFA ...) | 0 |

#### `gold.catalogo_dados`
*Catálogo de dados do projeto: uma linha por coluna das tabelas Silver e Gold.*  
**Linhagem:** gerada no notebook 03 a partir do schema de cada tabela (tipo), dos dicionários de descrição e linhagem definidos no notebook e de estatísticas calculadas sobre os dados (mínimo/máximo, categorias e % de nulos). Não usa joins.

| Coluna | Descrição | Tipo | Domínio |
|---|---|---|---|
| `camada` | Camada da tabela descrita. | string | {GOLD, SILVER} |
| `tabela` | Nome da tabela descrita. | string | tabelas Silver e Gold do projeto |
| `descricao_tabela` | Descrição da tabela. | string | texto livre |
| `linhagem` | Origem e transformações da tabela. | string | texto livre |
| `coluna` | Nome da coluna. | string | nomes de colunas |
| `descricao_coluna` | Descrição da coluna. | string | texto livre |
| `tipo` | Tipo de dado da coluna. | string | tipos Spark (string, date, int, boolean, decimal(20,2)…) |
| `dominio_observado` | Mínimo/máximo ou categorias observadas. | string | texto gerado |
| `pct_nulos` | Percentual de nulos da coluna. | double | [0 ; 100] |

**Observações sobre domínios**
- `fl_contribuicao_negativa` tem domínio `{false}` na Gold porque as 2 contribuições negativas de VGBL/PGBL da Silver são anteriores a 2014, fora do período do modelo.
- `vl_portab_aceita` tem mínimo negativo (−R$ 780 mil), que corresponde a um dos 5 valores negativos de portabilidade, mantidos e sinalizados na Silver.
- `dim_empresa` e `dim_grupo` trazem o cadastro completo do mercado; os fatos usam 30 empresas e 21 grupos.
- O catálogo das tabelas Silver está nos `COMMENT`s do Unity Catalog e em `gold.catalogo_dados` (filtro `camada = 'SILVER'`).

**Unity Catalog: colunas com comentários e PK/FK na interface**
![Catalog Explorer - colunas](docs/img/03_catalog_colunas.png)

**Linhagem capturada automaticamente pelo Unity Catalog** (`silver.contribuicoes`, `silver.resgates`, `silver.portabilidade` e `gold.ponte_empresa_grupo_mes` → `gold.fato_fluxo_previdencia`)
![Lineage](docs/img/03_lineage.png)

**Detalhes e propriedades Delta da tabela fato**
![Catalog details](docs/img/03_catalog_details.png)
![Catalog properties](docs/img/03_catalog_properties.png)

**Constraints PK/FK criadas** (CSV: [`docs/evidencias/constraints_gold.csv`](docs/evidencias/constraints_gold.csv); DESCRIBE completo: [`docs/evidencias/describe_fato_fluxo_previdencia.csv`](docs/evidencias/describe_fato_fluxo_previdencia.csv))
![Constraints](docs/img/03_constraints_status.png)

---

## 4. Pipeline de Dados

O pipeline foi organizado em quatro notebooks, executados em sequência no Databricks com computação *serverless*. Cada notebook lê as tabelas da camada anterior e grava tabelas Delta gerenciadas pelo Unity Catalog no catálogo `previdencia`. As tabelas de cada camada são sobrescritas a cada execução, então o pipeline pode ser reexecutado do início.

| Etapa | Notebook | Entrada | Saída | Principais transformações |
|---|---|---|---|---|
| Ingestão | [`01_bronze.py`](notebooks/01_bronze.py) | 9 CSVs no Volume | `bronze.*` (9 tabelas) | nenhuma no conteúdo; leitura `windows-1252`/`;`; metadados de ingestão; teste de contagem arquivo × tabela |
| Limpeza e qualidade | [`02_silver_qualidade.py`](notebooks/02_silver_qualidade.py) | `bronze.*` | `silver.empresa`, `empresa_grupo_mensal`, `contribuicoes`, `resgates`, `portabilidade`, `pmbac`, `dq_conciliacao_pgbl`, `dq_resultados`, `quarentena` | trim; padronização de nomes; vírgula→ponto; `DECIMAL(20,2)`; AAAAMM→`DATE`; remoção de duplicatas exatas (com prova de soma zero); agregação das sub-linhas pós-12/2013; união VGBL+PGBL; mapeamento R/D; *flags*; quarentena; 59 testes |
| Modelagem | [`03_gold_modelo_catalogo.py`](notebooks/03_gold_modelo_catalogo.py) | `silver.*` | `dim_*`, `ponte_empresa_grupo_mes`, `fato_fluxo_previdencia`, `fato_pmbac`, `catalogo_dados` | *as-of join* do grupo; união de chaves + `LEFT JOIN` das métricas; FLCR e captação líquida; PK/FK; 24 testes (unicidade, integridade, conservação de totais); `COMMENT`s |
| Consumo | [`04_analises.py`](notebooks/04_analises.py) | `gold.*` | consultas e gráficos | agregações anuais, jan–jul e mensais; taxa de resgate; HHI; rankings |

**Exemplos de transformações documentadas**
- *"Uni `ses_vgbl_resgates` e `ses_pgbl_resgates` com a coluna `cod_produto`, removi 15.029 duplicatas exatas após verificar que a soma removida era R$ 0,00 e somei as sub-linhas por empresa × mês × produto, porque a partir de 12/2013 a fonte passou a entregar até 3 linhas por chave sem coluna que as diferencie."*
- *"Fiz um* as-of join *das chaves dos fatos com `silver.empresa_grupo_mensal` para atribuir a cada fluxo o grupo econômico vigente no mês, porque 175 empresas mudaram de grupo ao longo do tempo."*
- *"Uni as chaves mês × empresa × produto de contribuições, resgates e portabilidade e associei cada origem por* left join*, porque há meses em que uma empresa tem resgate ou portabilidade sem contribuição. O teste de conservação confirma que os totais da Gold são idênticos aos da Silver (diferença de R$ 0,00 nas 5 métricas)."*

**Tabelas persistidas**
![Silver](docs/img/02_silver_tabelas.png)
![Gold](docs/img/03_gold_tabelas.png)

**Testes da Gold: 24/24 OK** (unicidade das PKs, integridade das FKs, conservação dos totais Silver → Gold e completude da atribuição de grupo). CSV: [`docs/evidencias/dq_resultados_gold.csv`](docs/evidencias/dq_resultados_gold.csv)
![Testes Gold](docs/img/03_testes_gold.png)

---

## 5. Qualidade de Dados

Cada problema foi diagnosticado com evidência antes de ser tratado, e nenhum valor foi alterado silenciosamente. Os registros que não puderam ser interpretados com segurança foram para `silver.quarentena`, e os valores atípicos legítimos foram mantidos e sinalizados com *flags*.

Os testes ficam gravados em `silver.dq_resultados` (camada, tabela, dimensão, regra, métrica, valor, esperado, status, ação). Na Silver foram executados 59 testes: 27 OK, 25 ALERTA (problema real da fonte, tratado e documentado), 7 INFO e nenhuma FALHA. Exportação: [`docs/evidencias/dq_resultados_silver.csv`](docs/evidencias/dq_resultados_silver.csv).

### 5.1 Problemas encontrados → evidência → tratamento

| # | Dimensão | Problema | Evidência (execução no Databricks) | Tratamento | Justificativa |
|---|---|---|---|---|---|
| 1 | Consistência | Encoding Windows-1252 | 225 nomes de empresas corrompidos (`�`) lidos como UTF-8; 0 lidos como windows-1252 | leitura com `encoding=windows-1252` | charset real do arquivo |
| 2 | Consistência | Vírgula decimal e resíduo de ponto flutuante | 90,4% dos valores de `contrib` com vírgula; ex.: `31557844,9700002` | vírgula→ponto; `DECIMAL(20,2)` via `try_cast` (0 valores não numéricos) | tipagem correta sem perder informação |
| 3 | Consistência | Período AAAAMM em texto | `damesano` | → `DATE` (1º dia do mês); 3 meses inválidos em quarentena | operações temporais |
| 4 | Consistência | Nomes de colunas inconsistentes | 3 variantes para a empresa (`coenti`, `COENTI`, `Coenti`) nos 9 arquivos | padronização para `cod_empresa`, `ano_mes`, `mes_ref` | joins e catálogo consistentes |
| 5 | Consistência | Espaços nas chaves | 100% das linhas de `Ses_cias` (769) e dos arquivos de fundos (7.122 e 6.184) | `trim` em todas as chaves | sem isso o join falha silenciosamente |
| 6 | Completude | Colunas 100% vazias | `Cogrupo`/`Nogrupo` (cadastro); `BENEFPAGO`/`NUMBENEF` (`ses_pgbl_uf`) | excluídas / não usadas | sem conteúdo; o grupo vem de `ses_grupos_economicos` |
| 7 | Consistência (schema × documentação) | Coluna não documentada | `Resg_Pag_programado` presente nos 2 arquivos de resgate e ausente da documentação SUSEP | mantida em coluna própria, fora de `vl_resgate` | transparência |
| 8 | Unicidade | Duplicatas exatas nos resgates | 15.029 linhas repetidas; soma removida = R$ 0,00 | removidas somente após provar que a soma removida é zero (caso contrário, o pipeline para) | não carregam informação |
| 9 | Consistência temporal | Mudança de granularidade | 1 linha por empresa × mês até 11/2013; a partir de 12/2013, até 3 sub-linhas sem coluna que as diferencie | soma por empresa × mês × produto | são parcelas do mesmo total. Teste de continuidade em 12/2013: VGBL 13,8% (p90 histórico 16,5%) e PGBL 5,3% (p90 64,2%), sem salto artificial |
| 10 | Consistência (domínio) | `TIPOTRANSF` fora do domínio | valores `D`, `R`, `r`, `P`; `r` e `P` somam 3 linhas / R$ 6,5 mi | R→ACEITA, D→CEDIDA (confirmado no SES online, §5.2); `r` e `P` em quarentena, sem interpretação | a documentação só diz "Aceita ou Cedida" |
| 11 | Completude / escopo | `TIPOPLANO` vazio e outras modalidades | 45 linhas sem produto (R$ 254,6 mi); VGBL+PGBL = 99,18% do valor portado | somente VGBL e PGBL no escopo; o restante é quantificado | foco do MVP |
| 12 | Acurácia | `QUANTIDADE` de portabilidades implausível | 22 linhas com valor médio < R$ 100 por portabilidade; ex.: empresa 06033, 12/2012: 2,75 bilhões de portabilidades (≈ R$ 0,001 cada) | flag `fl_qtd_portab_suspeita`; `QUANTIDADE` não é usada nas análises | limiar heurístico: a mediana é ≈ R$ 96,9 mil por portabilidade e o percentil 0,1% ≈ R$ 32 |
| 13 | Validade | Valores negativos | contribuições VGBL/PGBL: 2 linhas (−R$ 61,3 mi); portabilidade: 5 linhas | mantidos com flag | estornos/ajustes são legítimos no FIP |
| 14 | Consistência temporal | Grupo econômico muda no tempo | 175 empresas do mercado com mais de um grupo no histórico (74 das 124 de previdência); 3 linhas sem grupo e 3 com mês inválido (quarentena) | tabela empresa × mês + atribuição *as-of* na Gold (§3.1) | cada fluxo pertence ao grupo da época |
| 15 | Integridade referencial | Empresas órfãs | 0 em contribuições, resgates, portabilidade, PMBaC e grupos | — | joins da Gold sem perda |
| 16 | Integridade | PMBaC compatível com a taxa de resgate | 82 pares de resgate sem estoque (2014+), todos com R$ 0,00 | — | todo valor resgatado tem estoque correspondente, o que viabiliza a Q2 |
| 17 | Outliers | Contribuições atípicas | 17 empresa × mês com contribuição > 5× a média dos 12 meses anteriores e > R$ 50 mi, concentradas em PGBL em dezembro | mantidos | sazonalidade real: a contribuição ao PGBL é dedutível do IR no ano-calendário |
| 18 | Acurácia (conciliação entre fontes) | Duas fontes SUSEP para PGBL divergem | contribuições: 85,8% dos pares empresa × mês idênticos até 0,1% (87,8% até 1%); resgates: 71,4% até 0,1% (77,4% até 1%) | nenhuma fonte foi corrigida; `ses_contrib_benef` e os arquivos de resgates são as fontes oficiais, e `ses_pgbl_uf` serve só como verificação | quadros diferentes do FIP; a divergência maior nos resgates pode indicar diferença de conceito entre `RESGPAGO` (UF) e `resg_total + resg_parcial` (hipótese) |
| 19 | Acurácia (coerência) | Simetria da portabilidade no mercado | Σ aceita > Σ cedida em todos os anos; máximo de 15,6% (2017) e 2–4% desde 2018 → ALERTA (limite 15%) | mantido como ALERTA; o limiar não foi ajustado após o resultado | a diferença residual pode vir de fluxos com entidades/modalidades fora do escopo (hipótese). O saldo por grupo é válido, mas o saldo de mercado não é exatamente zero |

### 5.2 Direção da portabilidade (R/D) confirmada na fonte oficial

A documentação das tabelas informa apenas "Tipo de transferência (Aceita ou Cedida)", sem indicar qual letra corresponde a cada direção. O mapeamento foi confirmado na consulta oficial do SES online (*Previdência: Portabilidades Externas*, empresa 04031, grupo VGBL, período 202509):

| SES online | Valor | Quantidade | Linha no CSV |
|---|---|---|---|
| Valor Aceito | R$ 2.757.725.564 | 12.805 | `04031;202509;R;VGBL;2757725563,85;12805` |
| Valor Cedido | R$ 287.146.012 | 1.140 | `04031;202509;D;VGBL;287146011,54;1140` |

Portanto, R = aceita e D = cedida. Os códigos `r` e `P`, que não aparecem na documentação, não foram interpretados por analogia e ficaram em quarentena.

![SES online — portabilidade](docs/img/02_ses_online_portabilidade.png)

### 5.3 Evidências

**Resultado consolidado dos testes na Silver (`silver.dq_resultados`, 59 testes)**
![DQ resultados](docs/img/02_dq_resultados.png)

**Encoding: o mesmo arquivo lido como UTF-8 × windows-1252**
![Encoding](docs/img/02_encoding.png)

**Nomes de colunas inconsistentes entre arquivos → padronização**
![Nomes de colunas](docs/img/02_nomes_colunas.png)

**Colunas 100% vazias em `ses_pgbl_uf`**
![Colunas vazias](docs/img/02_pgbl_uf_colunas_vazias.png)

**Duplicatas exatas (soma removida = 0) e testes dos resgates agregados**
![Duplicatas](docs/img/02_duplicatas_resgates.png)
![Testes de resgates](docs/img/02_resgates_testes.png)

**Mudança de granularidade (linhas por empresa × mês, por ano)**
![Granularidade](docs/img/02_granularidade.png)

**Continuidade da série na quebra de 12/2013**
![Continuidade](docs/img/02_continuidade.png)

**Portabilidade: domínios, quantidade implausível e simetria de mercado**
![TIPOTRANSF](docs/img/02_tipotransf.png)
![Portabilidade - domínio](docs/img/02_portab_dominio.png)
![Portabilidade - quantidade suspeita](docs/img/02_portab_qtd_suspeita.png)
![Portabilidade - simetria](docs/img/02_portab_simetria.png)

**PMBaC: compatibilidade com a taxa de resgate**
![PMBaC](docs/img/02_pmbac.png)

**Conciliação PGBL entre fontes (geral e por ano)**
![Conciliação](docs/img/02_conciliacao.png)
![Conciliação por ano](docs/img/02_conciliacao_por_ano.png)

**Integridade referencial e outliers**
![Integridade](docs/img/02_integridade.png)
![Outliers](docs/img/02_outliers.png)

---

## 6. Análise de Dados

As respostas vêm das consultas Spark SQL do notebook [`04_analises.py`](notebooks/04_analises.py), executadas sobre a camada Gold. Valores em R$ bilhões nominais, sem correção pela inflação. As comparações anuais usam apenas anos completos (2014–2025), e 2026 é comparado somente em base jan–jul.

### Q1. Como evoluíram contribuições, resgates e FLCR de VGBL e PGBL?

| Ano | VGBL contrib. | VGBL resgates | **VGBL FLCR** | PGBL contrib. | PGBL resgates | **PGBL FLCR** |
|---|---|---|---|---|---|---|
| 2014 | 71,3 | 33,6 | **37,7** | 8,5 | 5,4 | **3,1** |
| 2016 | 105,0 | 45,0 | **60,0** | 8,9 | 7,1 | **1,8** |
| 2019 | 114,8 | 60,8 | **53,9** | 10,8 | 8,1 | **2,7** |
| 2021 | 126,2 | 92,7 | **33,5** | 11,6 | 9,7 | **2,0** |
| 2023 | 153,3 | 111,9 | **41,4** | 13,9 | 11,4 | **2,6** |
| 2024 | 178,3 | 119,2 | **59,0** | 15,3 | 12,6 | **2,7** |
| 2025 | 139,3 | 135,8 | **3,5** | 15,5 | 14,3 | **1,2** |

*(série anual completa no print abaixo)*

**Comparação jan–jul (base para 2026)**

| jan–jul | VGBL contrib. | VGBL resgates | VGBL FLCR | PGBL FLCR |
|---|---|---|---|---|
| 2023 | 86,1 | 66,5 | 19,6 | −0,8 |
| 2024 | 105,2 | 67,1 | **38,1** | −0,6 |
| 2025 | 92,3 | 78,3 | **14,1** | −2,1 |
| 2026 | 82,8 | 74,8 | **8,0** | −0,8 |

**Resposta.** No VGBL, as contribuições passaram de R$ 71,3 bi (2014) para R$ 178,3 bi (2024), cerca de 2,5 vezes mais. Em 2025 caíram 22%, para R$ 139,3 bi, enquanto os resgates subiram para R$ 135,8 bi. Com isso, o FLCR caiu de R$ 59,0 bi para R$ 3,5 bi, e a relação resgate/contribuição, que variou entre 0,43 e 0,78 de 2014 a 2024, chegou a 0,97. Na série mensal, o FLCR do VGBL ficou negativo em vários meses do 2º semestre de 2025. Em jan–jul/2026 o FLCR (R$ 8,0 bi) é o menor da série jan–jul calculada (2021–2026).

No PGBL, o FLCR anual foi positivo em todos os anos, com mínimo de R$ 1,2 bi em 2025. Já o FLCR de jan–jul foi negativo em todos os anos calculados (2021–2026), porque as contribuições se concentram em dezembro, quando o participante aproveita a dedução no IR do ano-calendário. Esse padrão também aparece entre os *outliers* da seção 5.

![Q1 anual](docs/img/04_q1_anual.png)
![Q1 gráfico](docs/img/04_q1_grafico.png)
![Q1 jan-jul](docs/img/04_q1_ytd.png)
![Q1 mensal](docs/img/04_q1_mensal_grafico.png)

### Q2. Como evoluiu a taxa de resgate (resgates ÷ PMBaC média)?

| | 2014 | 2016 | 2019 | 2020 | 2022 | 2024 | 2025 | jan–jul/2025 | jan–jul/2026 |
|---|---|---|---|---|---|---|---|---|---|
| **VGBL** | 12,5% | 10,4% | **8,7%** | 9,5% | **12,3%** | 10,1% | 10,1% | 6,0% | **5,1%** |
| **PGBL** | 6,3% | 6,3% | 5,3% | 5,3% | 6,2% | 5,6% | 5,6% | 3,6% | **3,0%** |
| PMBaC média VGBL (R$ bi) | 269 | 432 | 697 | 760 | 887 | 1.179 | 1.339 | 1.304 | 1.481 |

**Resposta.** No VGBL, a taxa de resgate caiu de 12,5% (2014) para 8,7% (2019), subiu até 12,3% (2022) e ficou em 10,1% em 2024 e em 2025. Em jan–jul/2026 foi de 5,1%, o menor valor da série jan–jul calculada (2021–2026). No PGBL, a taxa variou entre 5,3% e 6,5% ao longo do período.

Esse resultado ajuda a interpretar a Q1. Os resgates de VGBL aumentaram em valor (R$ 119,2 bi → R$ 135,8 bi), mas na mesma proporção do estoque. Assim, os dados indicam que a queda do FLCR em 2025–2026 está associada principalmente à redução das contribuições, e não a uma saída proporcionalmente maior de recursos.

![Q2 tabela](docs/img/04_q2_tabela.png)
![Q2 gráfico](docs/img/04_q2_grafico.png)

### Q3. Qual o grau de concentração das contribuições entre grupos econômicos?

| | 2014 | 2017 | 2020 | 2023 | 2025 |
|---|---|---|---|---|---|
| **HHI VGBL** | 2.774 | 2.505 | 2.220 | 2.235 | **2.026** |
| Top-5 VGBL | 96,2% | 96,5% | 94,4% | 92,5% | **91,4%** |
| **HHI PGBL** | 1.992 | 2.166 | 1.703 | 1.549 | **1.445** |
| Top-5 PGBL | 84,4% | 88,4% | 83,7% | 80,9% | **78,5%** |

| Posição | VGBL 2014 | VGBL 2025 | PGBL 2014 | PGBL 2025 |
|---|---|---|---|---|
| 1º | Brasil 39,7% | Brasil 30,1% | Brasil 29,1% | **Itaú 24,7%** |
| 2º | Bradesco 28,1% | Bradesco 24,2% | Bradesco 24,9% | Brasil 17,1% |
| 3º | Itaú 19,0% | **Caixa 18,4%** | Itaú 20,9% | Bradesco 14,2% |
| 4º | Zurich Santander* 4,9% | Itaú 10,4% | HSBC 4,8% | **Icatu 12,1%** |
| 5º | Caixa 4,5% | Zurich Santander* 8,2% | Caixa 4,7% | **XP*** 10,3% |

\* Empresas classificadas pela SUSEP no grupo genérico 99999, tratadas como unidade econômica própria.

**Resposta.** As contribuições continuam concentradas em poucos grupos, mas menos do que em 2014 nos dois produtos. No VGBL, o HHI das contribuições caiu 27% (2.774 → 2.026), e os 5 maiores grupos ainda respondem por mais de 90% das contribuições; a Caixa passou de 5º para 3º e o Itaú de 3º para 4º. No PGBL, o HHI também caiu 27% (1.992 → 1.445) e a participação dos 5 maiores passou de 84,4% para 78,5%; o Itaú assumiu a liderança, e Icatu e XP entraram entre os 5 maiores, no lugar de HSBC e Caixa. As participações foram calculadas com a atribuição temporal de grupo descrita na seção 3.1.

![Q3 tabela](docs/img/04_q3_tabela.png)
![Q3 gráfico](docs/img/04_q3_grafico.png)
![Q3 top-5](docs/img/04_q3_top5.png)

### Q4. Qual o saldo líquido de portabilidade por unidade econômica?

No total do mercado, o saldo (aceita − cedida) é pequeno diante do volume portado: em 2025 foram R$ 67,8 bi aceitos e R$ 65,9 bi cedidos, um saldo de +R$ 1,9 bi (ver item 19 da seção 5). O valor aceito mais que dobrou entre 2019 (R$ 31,7 bi) e 2025.

| 2025 (VGBL+PGBL) | Aceita | Cedida | **Saldo líquido** | Contribuições | Saldo ÷ contribuições |
|---|---|---|---|---|---|
| XP Vida e Previdência* | 22,9 | 5,9 | **+17,0** | 4,4 | 391% |
| Banco Pactual | 9,4 | 1,5 | **+7,8** | 3,4 | 234% |
| Itaú | 12,0 | 4,5 | **+7,5** | 18,3 | 41% |
| Safra | 3,0 | 2,1 | **+0,9** | 1,8 | 49% |
| Caixa | 2,4 | 3,9 | **−1,5** | 26,1 | −6% |
| Bradesco | 5,2 | 8,8 | **−3,6** | 36,0 | −10% |
| Sul América | 1,8 | 6,4 | **−4,6** | 0,7 | −665% |
| Brasil | 3,0 | 12,9 | **−9,9** | 44,7 | −22% |
| Icatu | 4,2 | 14,2 | **−10,0** | 5,7 | −177% |

**Resposta.** Em 2025, os maiores saldos líquidos positivos foram de XP (+R$ 17,0 bi), Banco Pactual (+R$ 7,8 bi) e Itaú (+R$ 7,5 bi), que somam R$ 32,4 bi. Os maiores saldos negativos foram de Icatu (−R$ 10,0 bi), Brasil (−R$ 9,9 bi), Sul América (−R$ 4,6 bi) e Bradesco (−R$ 3,6 bi). Para XP e Banco Pactual, o saldo de portabilidade foi maior que as contribuições do próprio ano (391% e 234%). Para Brasil, Bradesco e Caixa, o saldo negativo equivale a 6%–22% das contribuições. No Banco Pactual, o saldo ficou próximo de zero até 2019 (R$ 0,3 bi) e passou a ser positivo e crescente a partir de 2020 (R$ 0,9 bi), chegando a R$ 7,8 bi em 2025.

A base informa apenas os valores aceitos e cedidos por empresa, sem identificar a contraparte de cada transferência. Por isso, é possível dizer quais unidades tiveram saldo positivo ou negativo, mas não de qual unidade saíram os recursos recebidos por outra.

![Q4 mercado](docs/img/04_q4_mercado.png)
![Q4 tabela](docs/img/04_q4_tabela.png)
![Q4 gráfico](docs/img/04_q4_grafico.png)
![Q4 evolução](docs/img/04_q4_evolucao.png)

### Q5. Quanto o resultado e a posição mudam do FLCR para a captação líquida?

| 2025 (VGBL+PGBL) | FLCR | Saldo portab. | **Captação líquida** | Posição FLCR | Posição captação |
|---|---|---|---|---|---|
| XP Vida e Previdência* | −0,74 | +17,01 | **+16,27** | 18º | **1º** |
| Itaú | +4,15 | +7,49 | **+11,63** | 2º | 2º |
| Banco Pactual | +1,05 | +7,84 | **+8,89** | 4º | 3º |
| Caixa | +5,01 | −1,50 | **+3,51** | **1º** | 4º |
| Icatu | +1,27 | −9,99 | **−8,72** | 3º | **18º** |
| Brasil | −5,21 | −9,87 | **−15,08** | 19º | 19º |

**Resposta.** O FLCR considera apenas contribuições e resgates dos próprios participantes; a captação líquida soma a esse valor o saldo de portabilidade com outras entidades. Das 19 unidades com contribuições em 2025, 15 mudam de posição e 5 mudam de sinal quando a portabilidade é incluída. A XP passa de 18º (FLCR negativo) para 1º em captação líquida; a Icatu cai de 3º para 18º; a Caixa, primeira no FLCR, passa para 4º. O Brasil fica em último nas duas métricas, porque tem resgates maiores que as contribuições e saldo de portabilidade negativo. Para comparar unidades, portanto, é preciso deixar claro qual das duas métricas está sendo usada.

![Q5 tabela](docs/img/04_q5_tabela.png)
![Q5 gráfico](docs/img/04_q5_grafico.png)
![Q5 resumo](docs/img/04_q5_resumo.png)

### Discussão geral

O pipeline permitiu integrar contribuições, resgates, portabilidade, estoque e grupo econômico em uma base auditada e responder às cinco perguntas. Entre 2014 e 2024 as contribuições de VGBL cresceram cerca de 2,5 vezes, e a PMBaC média dos dois produtos passou de aproximadamente R$ 350 bi (2014) para R$ 1,6 tri (2025). Em 2025 o FLCR do VGBL caiu 94%, mas a taxa de resgate ficou estável; a mudança veio principalmente do lado das contribuições, e o quadro se manteve em jan–jul/2026. No mesmo período, a concentração das contribuições diminuiu, e a portabilidade, que movimentou R$ 67,8 bi aceitos em 2025, teve peso relevante no resultado de algumas unidades, a ponto de alterar o ranking entre FLCR e captação líquida.

**Contexto regulatório de 2025.** As contribuições de VGBL caíram de R$ 178,3 bi em 2024 para R$ 139,3 bi em 2025. No mesmo período houve mudanças na tributação de IOF sobre determinados aportes em VGBL (Decreto nº 12.499/2025, com suspensão e posterior restabelecimento cautelar pelo STF em julho de 2025). A coincidência temporal é relevante como contexto, mas não permite afirmar uma relação causal, cuja investigação está fora do escopo deste MVP.

**Confiabilidade.** As respostas se apoiam em 59 testes na Silver e 24 na Gold, sem falhas; na conservação de totais Silver → Gold (diferença de R$ 0,00); na direção da portabilidade confirmada na fonte oficial; e na conciliação de contribuições PGBL entre duas fontes da SUSEP (86–88%). As principais ressalvas são os valores nominais e a assimetria residual da portabilidade de mercado (+R$ 1,9 bi em 2025).

---

## 7. Autoavaliação

O MVP atingiu seu objetivo principal de construir um pipeline de dados em nuvem capaz de integrar e analisar os fluxos de VGBL e PGBL a partir dos dados do SES/SUSEP. A arquitetura Bronze → Silver → Gold permitiu preservar os dados originais, tratar problemas de qualidade de forma rastreável e disponibilizar uma camada modelada para responder às cinco perguntas de negócio.

Um dos principais aprendizados foi perceber que a maior dificuldade não estava no volume dos dados, mas na sua interpretação e integração. A mudança de granularidade dos arquivos de resgates, os domínios não documentados de portabilidade e as alterações de grupo econômico ao longo do tempo exigiram decisões que não poderiam ser resolvidas apenas com transformações automáticas.

Entre os principais desafios, destacam-se:

- **Granularidade dos resgates:** a partir de 12/2013, os arquivos passaram a apresentar múltiplas linhas por empresa e mês sem uma coluna que as diferenciasse. Foi necessário distinguir duplicatas exatas de sub-linhas legítimas antes da agregação.
- **Portabilidade:** a documentação não identifica diretamente o significado dos códigos `R` e `D`, o que exigiu validação na consulta oficial do SES. Códigos não documentados foram mantidos em quarentena.
- **Grupo econômico:** como empresas podem mudar de grupo ao longo do tempo, foi necessário fazer uma atribuição temporal para evitar que o grupo atual fosse aplicado retroativamente a todo o histórico.
- **Unidade econômica:** na primeira versão das análises Q4 e Q5, as empresas do código genérico `99999` foram excluídas, o que deixava de fora a XP, maior saldo positivo de portabilidade em 2025. A regra foi revisada para tratar cada empresa desse código como uma unidade econômica própria, como já era feito na Q3.
- **Ambiente:** as limitações do Databricks Free Edition levaram à ingestão manual dos arquivos para um Volume do Unity Catalog.

O resultado tem limitações importantes. Os valores são nominais e, portanto, as comparações de longo prazo não descontam a inflação. A conciliação entre diferentes fontes do SES não é integral, especialmente para os resgates de PGBL, e o saldo agregado de portabilidade não é perfeitamente simétrico. O VGBL não tem abertura por UF nas tabelas de previdência utilizadas, e as análises são descritivas, sem permitir relações causais.

Como evolução, seria possível incorporar correção monetária pelo IPCA, ampliar a análise geográfica, incluir a previdência tradicional e a quantidade de participantes e automatizar a atualização da base com um Job do Databricks e ingestão incremental. Análises sobre os efeitos de mudanças regulatórias também poderiam ser feitas futuramente, com metodologia apropriada.

De forma geral, o projeto mostrou que construir uma base analítica confiável depende não só da implementação do pipeline, mas também da compreensão da origem, da granularidade, da qualidade e do significado dos dados em cada etapa.

---

## Referências
- SUSEP — Sistema de Estatísticas da SUSEP (SES), base completa para download e documentação das tabelas: https://www2.susep.gov.br/menuestatistica/ses/principal.aspx
- Caixa Seguridade — Relatório de desempenho mensal SUSEP (metodologia de captação líquida sobre o SES): https://api.mziq.com/mzfilemanager/v2/d/3972906b-e50b-4f74-ab74-4d0d32125d11/ba4d6c1d-5f10-4cf0-9d59-ce0ae04abd2a?origin=2
- Decreto nº 12.499, de 11/06/2025 (IOF sobre seguros com cobertura por sobrevivência): https://www.planalto.gov.br/ccivil_03/_ato2023-2026/2025/decreto/d12499.htm
- Demarest Advogados — STF restabelece a eficácia do Decreto nº 12.499/2025 (decisão de 16/07/2025): https://www.demarest.com.br/majoracao-do-iof-stf-publica-decisao-cautelar-restabelecendo-a-eficacia-do-decreto-no-12-499-2025/
- Databricks — Free Edition limitations: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- Databricks — documentação sobre arquitetura medalhão e Unity Catalog (constraints, comentários e linhagem): https://docs.databricks.com
