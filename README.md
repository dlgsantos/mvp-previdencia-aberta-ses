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

| Sigla / termo | Significado |
|---|---|
| SUSEP | Superintendência de Seguros Privados, que regula seguros, previdência complementar aberta e capitalização |
| SES | Sistema de Estatísticas da SUSEP, base pública de onde vêm os dados deste trabalho |
| FIP | Formulário de Informações Periódicas, enviado pelas empresas à SUSEP; origem dos dados do SES |
| EAPP | Entidade Aberta de Previdência Complementar |
| PGBL | Plano Gerador de Benefício Livre: contribuições dedutíveis do IR (até 12% da renda bruta, declaração completa); o IR incide sobre o valor total resgatado |
| VGBL | Vida Gerador de Benefício Livre: sem dedução no IR; o imposto incide apenas sobre os rendimentos |
| Portabilidade | Transferência da reserva de um plano para outro, sem resgate. Aceita = recebida pela empresa; cedida = enviada |
| PMBaC | Provisão Matemática de Benefícios a Conceder: reserva dos participantes que ainda não recebem benefício (o estoque do plano) |
| Grupo econômico | Conjunto de empresas sob o mesmo controle |
| Código 99999 | Código genérico da SUSEP para empresas em "OUTROS GRUPOS"; não é um grupo real |
| Unidade econômica | Grupo econômico vigente no mês ou, para empresas do código 99999, a própria empresa |
| IOF | Imposto sobre Operações Financeiras |
| FLCR, captação líquida, taxa de resgate, HHI | Métricas definidas na seção 1 |
| YTD / jan–jul | Acumulado de janeiro a julho, usado para comparar 2026 com os anos anteriores |
| AAAAMM | Formato de mês do SES (ex.: 202507 = julho de 2025) |
| Bronze / Silver / Gold | Camadas da arquitetura medalhão: dado bruto → limpo e padronizado → modelado para análise |
| Unity Catalog | Catálogo de dados do Databricks: tabelas, comentários, permissões e linhagem |
| Quarentena | Tabela que guarda, sem descarte, os registros que não puderam ser interpretados com segurança |

---

## Visão geral do repositório

| Notebook | Conteúdo |
|---|---|
| [`notebooks/01_bronze.py`](notebooks/01_bronze.py) | Ingestão dos 9 CSVs do Volume para tabelas Delta, sem transformação, com metadados de rastreabilidade |
| [`notebooks/02_silver_qualidade.py`](notebooks/02_silver_qualidade.py) | Diagnóstico e tratamento da qualidade; tabelas Silver; quarentena; testes |
| [`notebooks/03_gold_modelo_catalogo.py`](notebooks/03_gold_modelo_catalogo.py) | Esquema estrela, PK/FK, testes da Gold e catálogo de dados |
| [`notebooks/04_analises.py`](notebooks/04_analises.py) | Consultas e visualizações que respondem às perguntas de negócio |

Os notebooks são executados em sequência (01 → 04) no Databricks Free Edition, no catálogo `previdencia` (schemas `bronze`, `silver` e `gold`). Prints em [`docs/img`](docs/img) e resultados exportados em [`docs/evidencias`](docs/evidencias).

---

## 1. Contexto de Negócios e Perguntas

### Contexto e problema
A previdência complementar aberta reúne planos oferecidos por seguradoras e EAPPs supervisionadas pela SUSEP. VGBL e PGBL, os dois principais produtos, somavam cerca de R$ 1,8 trilhão em PMBaC em julho de 2026 e movimentam centenas de bilhões de reais por ano em contribuições, resgates e portabilidades.

Esses dados são publicados no SES, mas espalhados em vários arquivos, com problemas de formato, granularidade e documentação (seção 5), e sem integração que permita acompanhar entradas e saídas por grupo econômico. Usá-los diretamente pode levar a erros de soma, à atribuição incorreta de grupos ao longo do tempo e à leitura equivocada dos fluxos.

### Objetivos
Construir no Databricks um pipeline Bronze → Silver → Gold reprodutível sobre o SES que:
1. ingira os arquivos preservando o original, com metadados de rastreabilidade;
2. diagnostique e trate, de forma documentada, os problemas de qualidade;
3. integre contribuições, resgates, portabilidade, provisões e grupo econômico com atribuição temporal;
4. disponibilize um esquema estrela catalogado;
5. responda às perguntas abaixo com Spark SQL.

### Perguntas de negócio
| # | Pergunta |
|---|---|
| Q1 | Como evoluíram contribuições, resgates e FLCR de VGBL e PGBL de 2014 a 2025, e como jan–jul/2026 se compara a jan–jul dos anos anteriores? |
| Q2 | Como evoluiu a taxa de resgate (resgates ÷ PMBaC média) de VGBL e PGBL? |
| Q3 | Qual o grau de concentração das contribuições entre grupos econômicos (participação dos 5 maiores e HHI), por produto e ano? |
| Q4 | Qual o saldo líquido de portabilidade por grupo econômico (unidade econômica) e qual a sua magnitude relativa às contribuições? |
| Q5 | Em que medida o resultado e a posição dos grupos mudam quando se passa do FLCR para a captação líquida (FLCR + saldo líquido de portabilidade)? |

### Definições das métricas
| Métrica | Fórmula | Origem | Limitações |
|---|---|---|---|
| Contribuições | Σ `contrib` (`tipoProd` ∈ {VGBL, PGBL}) | `Ses_Contrib_Benef` | Valores nominais |
| Resgates | Σ (`resg_total` + `resg_parcial`), sem duplicatas exatas e com as sub-linhas somadas | `Ses_vgbl_resgates`, `ses_pgbl_resgates` | `Resg_Pag_programado` (não documentado) fica fora da soma |
| FLCR | Contribuições − Resgates | as duas acima | Não inclui portabilidade, benefícios nem rentabilidade |
| Saldo líquido de portabilidade | Aceita (`TIPOTRANSF`=R) − Cedida (`TIPOTRANSF`=D) | `ses_transferenciasexternas` | Tende a ~0 no agregado do mercado |
| Captação líquida | FLCR + saldo líquido de portabilidade | as três acima | Conceito de mercado (ex.: Caixa Seguridade), não normativo; exclui benefícios |
| Taxa de resgate | Σ resgates do período ÷ média dos saldos mensais de PMBaC | resgates; `Ses_vgbl_fundos`, `ses_pgbl_fundos` | Denominador afetado por rentabilidade e portabilidade |
| HHI | Σ (participação × 100)² das contribuições | `Ses_Contrib_Benef`, `Ses_grupos_economicos`, `Ses_cias` | Vai de 0 (pulverizado) a 10.000 (monopólio) |

O grão de todas as métricas é empresa × mês × produto, agregável por grupo, produto e ano. Nas Q3–Q5, a unidade de análise é a **unidade econômica**: o grupo vigente no mês ou, para empresas do código 99999, a própria empresa, identificada como "(sem grupo)".

**Período:** jan/2014 a jul/2026. 2014 é o primeiro ano completo após a mudança de granularidade de 12/2013; as comparações anuais usam 2014–2025, e 2026 é comparado apenas em base jan–jul.

### Dados brutos, estrutura e licença
**Fonte:** SUSEP — SES, *Base de Dados do SES* (`BaseCompleta.zip`), gerada em 21/09/2026, com dados até 07/2026, e a documentação `Documentacao_das_tabelas.rtf`, ambas em https://www2.susep.gov.br/menuestatistica/ses/principal.aspx.

**Licença:** não há arquivo de licença específico no download. A base é disponibilizada publicamente pela SUSEP; aqui é usada para fins acadêmicos, com indicação da fonte, e os arquivos brutos não são redistribuídos no repositório.

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

\* Contagens confirmadas no notebook 01 (linhas no arquivo = linhas na tabela Bronze).

---

## 2. Carga dos Dados

1. Download manual do `BaseCompleta.zip` (40 CSVs) em 26/09/2026 e seleção dos 9 arquivos do escopo.
2. Upload para o Volume `/Volumes/previdencia/bronze/raw/ses/` pela interface do Databricks, já que o Free Edition restringe o acesso de saída à internet.
3. O notebook [`01_bronze.py`](notebooks/01_bronze.py) lê cada CSV com `sep=';'`, `encoding=windows-1252` e todas as colunas como texto, grava tabelas Delta em `previdencia.bronze.*` e acrescenta `_arquivo_origem`, `_data_geracao_base_ses`, `_mes_referencia_max`, `_ts_ingestao` e `_hash_linha`.
4. Teste de completude: linhas da tabela Bronze = linhas de dados do arquivo (9/9 OK).

![Volume raw](docs/img/01_volume_raw.png)
![Contagens Bronze](docs/img/01_bronze_contagens.png)
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
- **Um único fato de fluxo.** Contribuições, resgates e portabilidade têm o mesmo grão e se combinam no FLCR e na captação líquida. As chaves das três origens são unidas e cada origem é associada a elas, de modo que um mês com resgate ou portabilidade, mas sem contribuição, não se perde. Ausências viram 0, sinalizadas pelas *flags* `fl_tem_*`.
- **Fato de estoque separado.** A PMBaC é posição de fim de mês e não pode ser somada no tempo; por isso fica em `fato_pmbac`.
- **Grupo com atribuição temporal.** 175 empresas do mercado (74 das 124 de previdência) mudaram de grupo no histórico; a 05843, por exemplo, passou por Liberty, Talanx, Indiana, Bradesco e Independente. Aplicar o grupo atual a todo o período distorceria a concentração e os rankings. A `ponte_empresa_grupo_mes` define o grupo vigente em cada empresa × mês (*as-of join*: último grupo informado com mês ≤ mês do fato), e esse `cod_grupo` é gravado nos fatos. Resultado: 3.731 atribuições EXATAS, 1 HERDADA e 0 NAO_INFORMADO.
- **Chaves.** 7 PKs e 11 FKs criadas como *constraints* informativas no Unity Catalog, com validade verificada pelos testes da Gold (unicidade e órfãos = 0).

![Atribuição temporal de grupo](docs/img/03_ponte_atribuicao.png)
![Histórico de grupos por empresa](docs/img/02_grupos_temporais.png)

### 3.2 Catálogo de dados
O catálogo existe em três formas complementares:
1. `COMMENT` em cada tabela e coluna das camadas Silver e Gold no Unity Catalog, com descrição e linhagem;
2. tabela `gold.catalogo_dados` (notebook 03), com descrição, tipo, domínio observado (mínimo/máximo ou categorias) e % de nulos de cada coluna da Silver e da Gold. A parte da Gold está em [`docs/evidencias/catalogo_dados_gold.csv`](docs/evidencias/catalogo_dados_gold.csv);
3. linhagem capturada automaticamente pelo Unity Catalog (grafo abaixo).

Transcrição do catálogo do modelo Gold:

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


**Observações sobre domínios:** `fl_contribuicao_negativa` só tem `{false}` na Gold porque as 2 contribuições negativas da Silver são anteriores a 2014; o mínimo negativo de `vl_portab_aceita` (−R$ 780 mil) é um dos 5 valores negativos de portabilidade mantidos e sinalizados na Silver; `dim_empresa` e `dim_grupo` trazem o cadastro completo do mercado, enquanto os fatos usam 30 empresas e 21 grupos. O catálogo da Silver está nos `COMMENT`s do Unity Catalog e em `gold.catalogo_dados` (filtro `camada = 'SILVER'`).

![Catalog Explorer - colunas](docs/img/03_catalog_colunas.png)

Linhagem capturada pelo Unity Catalog (`silver.contribuicoes`, `silver.resgates`, `silver.portabilidade` e `gold.ponte_empresa_grupo_mes` → `gold.fato_fluxo_previdencia`):
![Lineage](docs/img/03_lineage.png)

Detalhes, propriedades Delta e *constraints* da tabela fato (CSVs: [`constraints_gold.csv`](docs/evidencias/constraints_gold.csv), [`describe_fato_fluxo_previdencia.csv`](docs/evidencias/describe_fato_fluxo_previdencia.csv)):
![Catalog details](docs/img/03_catalog_details.png)
![Catalog properties](docs/img/03_catalog_properties.png)
![Constraints](docs/img/03_constraints_status.png)

---

## 4. Pipeline de Dados

Quatro notebooks executados em sequência com computação *serverless*. Cada um lê a camada anterior e grava tabelas Delta gerenciadas pelo Unity Catalog, sobrescritas a cada execução.

| Etapa | Notebook | Entrada | Saída | Principais transformações |
|---|---|---|---|---|
| Ingestão | [`01_bronze.py`](notebooks/01_bronze.py) | 9 CSVs no Volume | `bronze.*` (9 tabelas) | nenhuma no conteúdo; leitura `windows-1252`/`;`; metadados de ingestão; teste de contagem |
| Limpeza e qualidade | [`02_silver_qualidade.py`](notebooks/02_silver_qualidade.py) | `bronze.*` | `silver.empresa`, `empresa_grupo_mensal`, `contribuicoes`, `resgates`, `portabilidade`, `pmbac`, `dq_conciliacao_pgbl`, `dq_resultados`, `quarentena` | trim; padronização de nomes; vírgula→ponto; `DECIMAL(20,2)`; AAAAMM→`DATE`; remoção de duplicatas exatas; agregação das sub-linhas; união VGBL+PGBL; mapeamento R/D; *flags*; quarentena; 59 testes |
| Modelagem | [`03_gold_modelo_catalogo.py`](notebooks/03_gold_modelo_catalogo.py) | `silver.*` | `dim_*`, `ponte_empresa_grupo_mes`, `fato_fluxo_previdencia`, `fato_pmbac`, `catalogo_dados` | *as-of join* do grupo; união de chaves + `LEFT JOIN` das métricas; FLCR e captação líquida; PK/FK; 24 testes; `COMMENT`s |
| Consumo | [`04_analises.py`](notebooks/04_analises.py) | `gold.*` | consultas e gráficos | agregações anuais, jan–jul e mensais; taxa de resgate; HHI; rankings |

Exemplo de transformação documentada: *"Uni `ses_vgbl_resgates` e `ses_pgbl_resgates` com a coluna `cod_produto`, removi 15.029 duplicatas exatas após verificar que a soma removida era R$ 0,00 e somei as sub-linhas por empresa × mês × produto, porque a partir de 12/2013 a fonte passou a entregar até 3 linhas por chave sem coluna que as diferencie."*

![Silver](docs/img/02_silver_tabelas.png)
![Gold](docs/img/03_gold_tabelas.png)

**Testes da Gold: 24/24 OK** (unicidade das PKs, integridade das FKs, conservação dos totais Silver → Gold com diferença de R$ 0,00 nas 5 métricas e completude da atribuição de grupo). CSV: [`dq_resultados_gold.csv`](docs/evidencias/dq_resultados_gold.csv)
![Testes Gold](docs/img/03_testes_gold.png)

---

## 5. Qualidade de Dados

Cada problema foi diagnosticado com evidência antes de ser tratado, e nenhum valor foi corrigido silenciosamente: registros que não puderam ser interpretados com segurança foram para `silver.quarentena`, e valores atípicos legítimos foram mantidos com *flags*. Os testes ficam em `silver.dq_resultados`; na Silver foram 59, com 27 OK, 25 ALERTA (problema real da fonte, tratado e documentado), 7 INFO e nenhuma FALHA ([`dq_resultados_silver.csv`](docs/evidencias/dq_resultados_silver.csv)).

### 5.1 Problemas encontrados e tratamento
Versão com dimensão de qualidade, evidência completa e justificativa: [`docs/qualidade_detalhada.md`](docs/qualidade_detalhada.md).

| # | Problema | Evidência | Tratamento |
|---|---|---|---|
| 1 | Encoding Windows-1252 | 225 nomes corrompidos lidos como UTF-8 | leitura com `windows-1252` |
| 2 | Vírgula decimal e resíduo de ponto flutuante | 90,4% dos valores de `contrib` com vírgula | vírgula→ponto; `DECIMAL(20,2)` (0 não numéricos) |
| 3 | Período AAAAMM em texto | `damesano` | → `DATE`; 3 meses inválidos em quarentena |
| 4 | Nomes de colunas inconsistentes | `coenti`, `COENTI`, `Coenti` | padronização (`cod_empresa`, `mes_ref`…) |
| 5 | Espaços nas chaves | 100% das linhas de `Ses_cias` e dos arquivos de fundos | `trim` em todas as chaves |
| 6 | Colunas 100% vazias | `Cogrupo`/`Nogrupo`; `BENEFPAGO`/`NUMBENEF` | excluídas / não usadas |
| 7 | Coluna não documentada | `Resg_Pag_programado` | coluna própria, fora de `vl_resgate` |
| 8 | Duplicatas exatas nos resgates | 15.029 linhas; soma removida = R$ 0,00 | removidas só após provar soma zero |
| 9 | Mudança de granularidade em 12/2013 | até 3 sub-linhas por empresa × mês | soma por chave; sem salto artificial na série |
| 10 | `TIPOTRANSF` fora do domínio | `D`, `R`, `r`, `P` | R→ACEITA, D→CEDIDA (§5.2); `r` e `P` em quarentena |
| 11 | `TIPOPLANO` vazio e outras modalidades | 45 linhas sem produto; VGBL+PGBL = 99,18% do valor | escopo VGBL/PGBL; restante quantificado |
| 12 | `QUANTIDADE` implausível | 22 linhas com < R$ 100 por portabilidade | flag; `QUANTIDADE` não usada |
| 13 | Valores negativos | 2 contribuições, 5 portabilidades | mantidos com flag (estornos) |
| 14 | Grupo muda no tempo | 175 empresas com mais de um grupo | atribuição *as-of* na Gold (§3.1) |
| 15 | Empresas órfãs | 0 em todas as tabelas | — |
| 16 | PMBaC × resgates | 82 pares sem estoque, todos com R$ 0,00 | — (viabiliza a Q2) |
| 17 | Contribuições atípicas | 17 casos, sobretudo PGBL em dezembro | mantidos (sazonalidade do IR) |
| 18 | Duas fontes de PGBL divergem | contribuições 86–88% dos pares iguais; resgates 71–77% | nenhuma corrigida; `ses_pgbl_uf` só como verificação |
| 19 | Assimetria da portabilidade no mercado | aceita > cedida; máx. 15,6% (2017), 2–4% desde 2018 | ALERTA mantido, limiar não ajustado |

### 5.2 Direção da portabilidade (R/D) confirmada na fonte oficial
A documentação diz apenas "Tipo de transferência (Aceita ou Cedida)", sem indicar a letra de cada direção. A consulta *Previdência: Portabilidades Externas* do SES online (empresa 04031, VGBL, 202509) resolve a dúvida:

| SES online | Valor | Quantidade | Linha no CSV |
|---|---|---|---|
| Valor Aceito | R$ 2.757.725.564 | 12.805 | `04031;202509;R;VGBL;2757725563,85;12805` |
| Valor Cedido | R$ 287.146.012 | 1.140 | `04031;202509;D;VGBL;287146011,54;1140` |

Logo, R = aceita e D = cedida. Os códigos `r` e `P` não foram interpretados por analogia.

![SES online — portabilidade](docs/img/02_ses_online_portabilidade.png)

### 5.3 Evidências
Resultado consolidado dos testes, encoding, nomes de colunas e colunas vazias:
![DQ resultados](docs/img/02_dq_resultados.png)
![Encoding](docs/img/02_encoding.png)
![Nomes de colunas](docs/img/02_nomes_colunas.png)
![Colunas vazias](docs/img/02_pgbl_uf_colunas_vazias.png)

Resgates: duplicatas, testes, granularidade e continuidade em 12/2013:
![Duplicatas](docs/img/02_duplicatas_resgates.png)
![Testes de resgates](docs/img/02_resgates_testes.png)
![Granularidade](docs/img/02_granularidade.png)
![Continuidade](docs/img/02_continuidade.png)

Portabilidade: domínios, quantidade implausível e simetria:
![TIPOTRANSF](docs/img/02_tipotransf.png)
![Portabilidade - domínio](docs/img/02_portab_dominio.png)
![Portabilidade - quantidade suspeita](docs/img/02_portab_qtd_suspeita.png)
![Portabilidade - simetria](docs/img/02_portab_simetria.png)

PMBaC, conciliação PGBL, integridade referencial e outliers:
![PMBaC](docs/img/02_pmbac.png)
![Conciliação](docs/img/02_conciliacao.png)
![Conciliação por ano](docs/img/02_conciliacao_por_ano.png)
![Integridade](docs/img/02_integridade.png)
![Outliers](docs/img/02_outliers.png)

---

## 6. Análise de Dados

Consultas Spark SQL do notebook [`04_analises.py`](notebooks/04_analises.py) sobre a camada Gold. Valores em R$ bilhões nominais.

### Q1. Como evoluíram contribuições, resgates e FLCR de VGBL e PGBL?

| Ano | VGBL contrib. | VGBL resgates | VGBL FLCR | PGBL contrib. | PGBL resgates | PGBL FLCR |
|---|---|---|---|---|---|---|
| 2014 | 71,3 | 33,6 | 37,7 | 8,5 | 5,4 | 3,1 |
| 2016 | 105,0 | 45,0 | 60,0 | 8,9 | 7,1 | 1,8 |
| 2019 | 114,8 | 60,8 | 53,9 | 10,8 | 8,1 | 2,7 |
| 2021 | 126,2 | 92,7 | 33,5 | 11,6 | 9,7 | 2,0 |
| 2023 | 153,3 | 111,9 | 41,4 | 13,9 | 11,4 | 2,6 |
| 2024 | 178,3 | 119,2 | 59,0 | 15,3 | 12,6 | 2,7 |
| 2025 | 139,3 | 135,8 | **3,5** | 15,5 | 14,3 | 1,2 |

| jan–jul | VGBL contrib. | VGBL resgates | VGBL FLCR | PGBL FLCR |
|---|---|---|---|---|
| 2023 | 86,1 | 66,5 | 19,6 | −0,8 |
| 2024 | 105,2 | 67,1 | 38,1 | −0,6 |
| 2025 | 92,3 | 78,3 | 14,1 | −2,1 |
| 2026 | 82,8 | 74,8 | **8,0** | −0,8 |

As contribuições de VGBL passaram de R$ 71,3 bi (2014) para R$ 178,3 bi (2024) e caíram 22% em 2025, enquanto os resgates subiram para R$ 135,8 bi; o FLCR caiu de R$ 59,0 bi para R$ 3,5 bi, e a relação resgate/contribuição, entre 0,43 e 0,78 até 2024, chegou a 0,97. Em jan–jul/2026 o FLCR do VGBL (R$ 8,0 bi) é o menor da série jan–jul calculada (2021–2026). No PGBL, o FLCR anual foi positivo em todos os anos, enquanto o acumulado de jan–jul foi negativo entre 2021 e 2026. O resultado é compatível com a concentração das contribuições no fim do ano, período relevante para o aproveitamento da dedução fiscal do PGBL.

![Q1 anual](docs/img/04_q1_anual.png)
![Q1 gráfico](docs/img/04_q1_grafico.png)
![Q1 jan-jul](docs/img/04_q1_ytd.png)
![Q1 mensal](docs/img/04_q1_mensal_grafico.png)

### Q2. Como evoluiu a taxa de resgate (resgates ÷ PMBaC média)?

| | 2014 | 2016 | 2019 | 2020 | 2022 | 2024 | 2025 | jan–jul/2025 | jan–jul/2026 |
|---|---|---|---|---|---|---|---|---|---|
| VGBL | 12,5% | 10,4% | 8,7% | 9,5% | 12,3% | 10,1% | 10,1% | 6,0% | 5,1% |
| PGBL | 6,3% | 6,3% | 5,3% | 5,3% | 6,2% | 5,6% | 5,6% | 3,6% | 3,0% |
| PMBaC média VGBL (R$ bi) | 269 | 432 | 697 | 760 | 887 | 1.179 | 1.339 | 1.304 | 1.481 |

A taxa do VGBL caiu de 12,5% (2014) para 8,7% (2019), subiu até 12,3% (2022) e ficou em 10,1% em 2024 e 2025; em jan–jul/2026 (5,1%) é a menor da série jan–jul calculada. No PGBL variou entre 5,3% e 6,5%. Os resgates de VGBL cresceram em valor em 2025, mas na mesma proporção do estoque, o que indica que a queda do FLCR está associada principalmente à redução das contribuições, e não a uma saída proporcionalmente maior de recursos.

![Q2 tabela](docs/img/04_q2_tabela.png)
![Q2 gráfico](docs/img/04_q2_grafico.png)

### Q3. Qual o grau de concentração das contribuições entre grupos econômicos?

| | 2014 | 2017 | 2020 | 2023 | 2025 |
|---|---|---|---|---|---|
| HHI VGBL | 2.774 | 2.505 | 2.220 | 2.235 | 2.026 |
| Top-5 VGBL | 96,2% | 96,5% | 94,4% | 92,5% | 91,4% |
| HHI PGBL | 1.992 | 2.166 | 1.703 | 1.549 | 1.445 |
| Top-5 PGBL | 84,4% | 88,4% | 83,7% | 80,9% | 78,5% |

| Posição | VGBL 2014 | VGBL 2025 | PGBL 2014 | PGBL 2025 |
|---|---|---|---|---|
| 1º | Brasil 39,7% | Brasil 30,1% | Brasil 29,1% | Itaú 24,7% |
| 2º | Bradesco 28,1% | Bradesco 24,2% | Bradesco 24,9% | Brasil 17,1% |
| 3º | Itaú 19,0% | Caixa 18,4% | Itaú 20,9% | Bradesco 14,2% |
| 4º | Zurich Santander* 4,9% | Itaú 10,4% | HSBC 4,8% | Icatu 12,1% |
| 5º | Caixa 4,5% | Zurich Santander* 8,2% | Caixa 4,7% | XP* 10,3% |

\* Empresas do código 99999, tratadas como unidade econômica própria.

As contribuições seguem concentradas, mas menos que em 2014. O HHI caiu 27% nos dois produtos (VGBL 2.774 → 2.026; PGBL 1.992 → 1.445). No VGBL os 5 maiores ainda somam mais de 90%, e a Caixa passou de 5º para 3º; no PGBL o top-5 caiu para 78,5%, o Itaú assumiu a liderança e Icatu e XP entraram no lugar de HSBC e Caixa.

![Q3 tabela](docs/img/04_q3_tabela.png)
![Q3 gráfico](docs/img/04_q3_grafico.png)
![Q3 top-5](docs/img/04_q3_top5.png)

### Q4. Qual o saldo líquido de portabilidade por unidade econômica?

| 2025 (VGBL+PGBL) | Aceita | Cedida | Saldo líquido | Contribuições | Saldo ÷ contribuições |
|---|---|---|---|---|---|
| XP Vida e Previdência* | 22,9 | 5,9 | +17,0 | 4,4 | 391% |
| Banco Pactual | 9,4 | 1,5 | +7,8 | 3,4 | 234% |
| Itaú | 12,0 | 4,5 | +7,5 | 18,3 | 41% |
| Safra | 3,0 | 2,1 | +0,9 | 1,8 | 49% |
| Caixa | 2,4 | 3,9 | −1,5 | 26,1 | −6% |
| Bradesco | 5,2 | 8,8 | −3,6 | 36,0 | −10% |
| Sul América | 1,8 | 6,4 | −4,6 | 0,7 | −665% |
| Brasil | 3,0 | 12,9 | −9,9 | 44,7 | −22% |
| Icatu | 4,2 | 14,2 | −10,0 | 5,7 | −177% |

No mercado, o saldo é pequeno diante do volume portado (2025: R$ 67,8 bi aceitos, R$ 65,9 bi cedidos, saldo de +R$ 1,9 bi), e o valor aceito mais que dobrou desde 2019 (R$ 31,7 bi). Por unidade, os maiores saldos positivos em 2025 foram de XP (+R$ 17,0 bi), Banco Pactual (+R$ 7,8 bi) e Itaú (+R$ 7,5 bi), e os maiores negativos de Icatu (−R$ 10,0 bi) e Brasil (−R$ 9,9 bi). Para XP e Banco Pactual o saldo superou as contribuições do ano; para Brasil, Bradesco e Caixa, equivale a 6%–22% delas. Como a base não identifica a contraparte de cada transferência, não é possível dizer de qual unidade saíram os recursos recebidos por outra.

![Q4 mercado](docs/img/04_q4_mercado.png)
![Q4 tabela](docs/img/04_q4_tabela.png)
![Q4 gráfico](docs/img/04_q4_grafico.png)
![Q4 evolução](docs/img/04_q4_evolucao.png)

### Q5. Quanto o resultado e a posição mudam do FLCR para a captação líquida?

| 2025 (VGBL+PGBL) | FLCR | Saldo portab. | Captação líquida | Posição FLCR | Posição captação |
|---|---|---|---|---|---|
| XP Vida e Previdência* | −0,74 | +17,01 | +16,27 | 18º | 1º |
| Itaú | +4,15 | +7,49 | +11,63 | 2º | 2º |
| Banco Pactual | +1,05 | +7,84 | +8,89 | 4º | 3º |
| Caixa | +5,01 | −1,50 | +3,51 | 1º | 4º |
| Icatu | +1,27 | −9,99 | −8,72 | 3º | 18º |
| Brasil | −5,21 | −9,87 | −15,08 | 19º | 19º |

O FLCR considera só contribuições e resgates dos próprios participantes; a captação líquida soma a ele o saldo de portabilidade. Das 19 unidades com contribuições em 2025, 15 mudam de posição e 5 mudam de sinal entre as duas métricas: a XP vai de 18º a 1º, a Icatu de 3º a 18º e a Caixa, primeira no FLCR, cai para 4º. Comparações entre unidades precisam, portanto, deixar claro qual métrica usam.

![Q5 tabela](docs/img/04_q5_tabela.png)
![Q5 gráfico](docs/img/04_q5_grafico.png)
![Q5 resumo](docs/img/04_q5_resumo.png)

### Discussão geral
Em 2025 o FLCR do VGBL caiu 94% com taxa de resgate estável, ou seja, a mudança veio sobretudo das contribuições, e o quadro se manteve em jan–jul/2026. A concentração das contribuições diminuiu, e a portabilidade teve peso suficiente para alterar o ranking de várias unidades. No mesmo ano houve mudanças na tributação de IOF sobre aportes em VGBL (Decreto nº 12.499/2025, com restabelecimento cautelar pelo STF em julho de 2025); a coincidência temporal é contexto relevante, mas não permite afirmar causalidade.

As respostas se apoiam nos 83 testes sem falhas, na conservação de totais Silver → Gold, na direção da portabilidade confirmada na fonte e na conciliação de contribuições PGBL entre duas fontes da SUSEP. As principais ressalvas são os valores nominais e a assimetria residual da portabilidade de mercado.

---

## 7. Autoavaliação

O MVP atingiu o objetivo de construir um pipeline em nuvem que integra e analisa os fluxos de VGBL e PGBL a partir do SES. A arquitetura em camadas preservou os dados originais, tornou o tratamento de qualidade rastreável e entregou uma camada modelada capaz de responder às cinco perguntas.

O principal aprendizado foi que a dificuldade não estava no volume, e sim na interpretação e integração dos dados. Os desafios que mais exigiram decisões foram:
- **Granularidade dos resgates:** a partir de 12/2013 surgiram várias linhas por empresa e mês sem coluna que as diferencie, e foi preciso separar duplicatas exatas de sub-linhas legítimas.
- **Portabilidade:** o significado de `R` e `D` teve de ser validado no SES online, e os códigos não documentados ficaram em quarentena.
- **Grupo econômico:** a atribuição temporal evitou aplicar o grupo atual a todo o histórico.
- **Unidade econômica:** a primeira versão das Q4 e Q5 excluía as empresas do código 99999, deixando de fora a XP, maior saldo positivo de 2025. A regra foi alinhada à da Q3, tratando cada uma como unidade própria.
- **Ambiente:** as restrições do Free Edition levaram à ingestão manual via Volume.

As limitações são os valores nominais, a conciliação parcial entre fontes (sobretudo nos resgates de PGBL), a assimetria da portabilidade agregada, a falta de abertura por UF para o VGBL e o caráter descritivo das análises. Como evolução, caberiam correção pelo IPCA, análise geográfica, previdência tradicional, número de participantes e atualização automática com Jobs e ingestão incremental.

O projeto mostrou que uma base analítica confiável depende tanto do pipeline quanto da compreensão da origem, da granularidade e do significado dos dados em cada etapa.

---

## Referências
- SUSEP — Sistema de Estatísticas da SUSEP (SES), base completa e documentação das tabelas: https://www2.susep.gov.br/menuestatistica/ses/principal.aspx
- Caixa Seguridade — Relatório de desempenho mensal SUSEP (metodologia de captação líquida sobre o SES): https://api.mziq.com/mzfilemanager/v2/d/3972906b-e50b-4f74-ab74-4d0d32125d11/ba4d6c1d-5f10-4cf0-9d59-ce0ae04abd2a?origin=2
- Decreto nº 12.499, de 11/06/2025 (IOF sobre seguros com cobertura por sobrevivência): https://www.planalto.gov.br/ccivil_03/_ato2023-2026/2025/decreto/d12499.htm
- Demarest Advogados — STF restabelece a eficácia do Decreto nº 12.499/2025 (decisão de 16/07/2025): https://www.demarest.com.br/majoracao-do-iof-stf-publica-decisao-cautelar-restabelecendo-a-eficacia-do-decreto-no-12-499-2025/
- Databricks — Free Edition limitations: https://docs.databricks.com/aws/en/getting-started/free-edition-limitations
- Databricks — documentação sobre arquitetura medalhão e Unity Catalog: https://docs.databricks.com
