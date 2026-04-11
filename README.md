# smartphone-pipeline

Pipeline de dados para monitoramento de preços de smartphones na Amazon Brasil. Coleta listagens via RapidAPI, processa os dados por meio de uma arquitetura de streaming e transformação em camadas, e os disponibiliza em um dashboard analítico interativo.

## Contexto de negócio

Uma startup de e-commerce especializada em acessórios para smartphones precisa de inteligência competitiva para embasar suas decisões de precificação. Para isso, o time de dados deve responder dez perguntas de negócio sobre o mercado de smartphones no Brasil — distribuição de preços, comportamento do frete grátis, ranking de marcas, correlação entre desconto e volume de vendas e evolução temporal dos preços.

Este pipeline automatiza a coleta, transformação e visualização desses dados, atualizando o dashboard a cada hora.

---

## Arquitetura do pipeline

```
Amazon BR (RapidAPI)
        |
        v
  [ Collector ]          Python + httpx + tenacity
        |                Coleta ate 480 produtos/hora (10 paginas x 48 produtos)
        | XADD
        v
  [ Redis Stream ]       smartphones_raw
        |                Dead letter: smartphones_failed (apos 3 tentativas)
        | XREADGROUP
        v
  [ Consumer ]           Python + psycopg2
        |                Garante at-least-once via XACK somente apos persistencia
        | INSERT
        v
  [ PostgreSQL / raw ]   raw.products (JSONB + colunas normalizadas)
        |
        | dbt run --select staging
        v
  [ PostgreSQL / staging ]  staging.stg_products (view)
        |                   Deduplicacao, tipagem, extracao de marca, normalizacao
        | dbt run --select marts
        v
  [ PostgreSQL / marts ]    fct_products, dim_sellers, agg_* (tables/incremental)
        |
        v
  [ Dashboard Streamlit ]   http://localhost:8501
                            3 paginas, 10 perguntas de negocio, Plotly
```

### Componentes e responsabilidades

| Componente | Tecnologia | Responsabilidade |
|---|---|---|
| Collector | Python 3.11, httpx, tenacity | Busca produtos na Amazon BR via RapidAPI e publica cada item no Redis Stream |
| Redis Stream | Redis 7 | Fila duravel com consumer groups, garantia de entrega e dead letter stream |
| Consumer | Python 3.11, psycopg2 | Le o stream, persiste em `raw.products` e confirma com XACK |
| PostgreSQL | PostgreSQL 15 | Armazenamento em tres schemas: raw, staging, marts |
| dbt Core | dbt 1.7+ | Transformacoes SQL versionadas com testes de qualidade nativos |
| Orchestration | Prefect 2.x | Agendamento horario, retries por etapa e monitoramento via UI |
| Dashboard | Streamlit 1.32+, Plotly 5 | Visualizacoes interativas com filtros globais |

---

## Decisoes arquiteturais

### Por que Redis Streams e nao Apache Kafka?

Kafka e a escolha padrao para streaming em alta escala, mas exige no minimo tres servicos (Zookeeper, Broker e Schema Registry ou UI), consome mais de 1 GB de RAM e demanda configuracao significativa antes de qualquer linha de codigo de negocio ser escrita.

Redis resolve o mesmo problema — filas duraveis, consumer groups, at-least-once delivery e dead letter stream — com um unico servico que consome menos de 100 MB. Para um volume de 480 produtos por hora, Redis e a escolha tecnicamente correta. Escalar para Kafka faria sentido a partir de dezenas de milhares de mensagens por minuto, cenario que nao se aplica aqui.

As garantias implementadas com Redis Streams sao equivalentes as do Kafka para este caso de uso: mensagens persistidas no disco, reentrega automatica de mensagens nao confirmadas via `XAUTOCLAIM` e isolamento de falhas por consumer group.

### Por que Prefect e nao Apache Airflow?

Airflow e robusto para orquestracao de pipelines complexos, mas exige quatro ou mais servicos (webserver, scheduler, worker, banco de metadados), configuracao de DAGs em arquivos separados do codigo de negocio e uma curva de aprendizado consideravel.

Prefect permite definir flows e tasks com decorators Python diretamente no mesmo arquivo que implementa a logica. O setup e feito em minutos, a UI e moderna e o servidor pode ser iniciado com um unico container. Para um pipeline linear de seis etapas com agendamento horario, Prefect oferece todos os recursos necessarios sem a complexidade operacional do Airflow.

### Por que PostgreSQL e nao DuckDB ou BigQuery?

PostgreSQL oferece tres caracteristicas decisivas para este projeto. Primeiro, suporte nativo a JSONB, que permite armazenar o payload bruto da API na camada Raw sem perda de dados. Segundo, compatibilidade completa com dbt Core e todos os seus adaptadores. Terceiro, execucao local dentro do Docker sem custo e sem dependencia de servicos externos.

DuckDB seria uma alternativa valida para analise local, mas nao oferece suporte a streaming e consumer groups. BigQuery adicionaria latencia de rede, custo por consulta e dependencia de credenciais de nuvem — barreiras desnecessarias para um pipeline que opera localmente.

### Por que dbt Core e nao scripts pandas?

Scripts pandas resolvem transformacoes pontuais, mas nao escalam como pratica de engenharia de dados. dbt oferece quatro vantagens estruturais: testes de qualidade nativos que falham o pipeline quando os dados violam contratos definidos; documentacao e linhagem geradas automaticamente a partir dos modelos SQL; versionamento completo no Git com historico de mudancas auditavel; e separacao clara entre transformacao (dbt) e orquestracao (Prefect), evitando codigo misto que mistura logica de negocio com logica de agendamento.

### Por que Streamlit e nao Power BI ou Metabase?

Power BI nao roda em containers Docker e exige licenca para publicacao. Metabase e uma ferramenta valida, mas adiciona um servico extra ao compose e nao e versionavel como codigo.

Streamlit e codigo Python puro: pode ser revisado em pull request, testado com pytest, versionado no Git e containerizado com um Dockerfile de dez linhas. O dashboard inteiro esta em quatro arquivos Python e um modulo de utilitarios compartilhados — qualquer desenvolvedor do time pode ler, modificar e implantar sem ferramenta externa.

### Por que Amazon BR via RapidAPI e nao a API oficial do Mercado Livre?

A API oficial do Mercado Livre retorna `403 Forbidden` para aplicacoes novas que ainda nao passaram pelo processo de aprovacao manual da plataforma — um processo que pode levar semanas e cujo resultado e imprevisivel para contas recentes. Esse comportamento foi documentado por multiplos desenvolvedores em 2025 e 2026 em foros publicos e no GitHub Issues do SDK oficial.

A Real-Time Amazon Data API via RapidAPI retorna dados reais em JSON estruturado com todos os campos necessarios para as dez perguntas de negocio. O plano gratuito oferece 100 requisicoes por mes, suficiente para validar o pipeline e demonstrar o funcionamento completo do projeto.

Para uso em producao, recomenda-se assinar um plano pago da mesma API ou avaliar fontes alternativas como web scraping proprio (respeitando os termos de uso) ou parceria com agregadores de dados de e-commerce.

---

## Fonte de dados

**API:** Real-Time Amazon Data (RapidAPI)
**Dominio:** www.amazon.com.br
**Endpoint:** `/search`
**Frequencia:** a cada hora (agendado pelo Prefect)
**Volume:** ate 480 produtos por execucao (10 paginas x 48 produtos)

### Campos coletados

| Campo | Tipo | Descricao |
|---|---|---|
| `product_id` | TEXT | ASIN do produto (identificador unico Amazon) |
| `title` | TEXT | Titulo completo do anuncio |
| `brand` | TEXT | Marca extraida do titulo via heuristica |
| `condition` | TEXT | Condicao do produto (`new` ou `used`) |
| `price` | NUMERIC | Preco atual em BRL |
| `original_price` | NUMERIC | Preco original antes do desconto |
| `discount_pct` | NUMERIC | Percentual de desconto calculado |
| `rating` | NUMERIC | Avaliacao media (0.0 a 5.0) |
| `num_ratings` | INTEGER | Numero total de avaliacoes |
| `is_best_seller` | BOOLEAN | Indicador de Best Seller |
| `is_amazon_choice` | BOOLEAN | Indicador de Amazon's Choice |
| `is_prime` | BOOLEAN | Indicador de elegibilidade Prime |
| `free_shipping` | BOOLEAN | Inferido do campo `delivery_text` |
| `sales_volume` | TEXT | Texto de volume de vendas (ex: "Mais de 1,5 mil compras") |
| `url` | TEXT | URL do produto |
| `thumbnail_url` | TEXT | URL da imagem principal |

### Limitacoes da fonte

**Brand como proxy de vendedor (P3 e P10):** O endpoint `/search` da Amazon nao retorna dados do vendedor individual. A marca e extraida do titulo por heuristica (prefixo de 9 marcas conhecidas) e usada como proxy nos modelos `dim_sellers` e nas analises de competitividade. Em producao, o endpoint `/product-details` retorna o seller real, mas exige uma chamada por produto — inviavel dentro do limite de 100 requisicoes mensais do plano gratuito.

**Limite de 100 requisicoes mensais:** O plano gratuito da RapidAPI e suficiente para validacao. Em producao, o plano basico pago oferece 1.000 requisicoes mensais por aproximadamente USD 10.

---

## Pre-requisitos

- Docker Desktop instalado e em execucao (versao 24 ou superior)
- Conta gratuita no [RapidAPI](https://rapidapi.com) com assinatura da API **Real-Time Amazon Data**
- Chave de API (`X-RapidAPI-Key`) obtida no painel do RapidAPI
- Git
- Make (disponivel por padrao no macOS e na maioria das distribuicoes Linux)

---

## Como rodar do zero

### Passo 1: Clonar o repositorio

```bash
git clone https://github.com/luiz-data/smartphone-pipeline.git
cd smartphone-pipeline
```

### Passo 2: Configurar as variaveis de ambiente

```bash
cp .env.example .env
```

Abra o arquivo `.env` e preencha os valores obrigatorios:

```
POSTGRES_PASSWORD=escolha_uma_senha_segura
RAPIDAPI_KEY=sua_chave_rapidapi_aqui
```

As demais variaveis ja possuem valores padrao funcionais.

### Passo 3: Construir as imagens Docker

```bash
make build
```

Este comando constroi as imagens de todos os servicos (collector, consumer, orchestration, dashboard) sem cache.

### Passo 4: Subir os containers

```bash
make up
```

Inicia todos os seis servicos em background: postgres, redis, consumer, orchestration, prefect-server e dashboard.

### Passo 5: Aguardar os servicos ficarem saudaveis

```bash
make health
```

Aguarde ate que postgres, redis e prefect-server apareçam com status `healthy`. Isso pode levar entre 30 e 60 segundos na primeira execucao.

### Passo 6: Carregar o seed historico

```bash
make seed
```

Carrega 35 linhas de precos historicos simulados (5 produtos x 7 dias) necessarias para que os graficos de evolucao temporal (P5 e P7) funcionem antes que dados reais de multiplos dias sejam acumulados.

### Passo 7: Executar as transformacoes dbt

```bash
make dbt-run
```

Materializa todos os modelos dbt: a view `stg_products` na camada staging e as tabelas analíticas na camada marts.

### Passo 8: Acessar o dashboard

```
http://localhost:8501
```

### Passo 9: Acessar a UI do Prefect

```
http://localhost:4200
```

A partir daqui, o pipeline roda automaticamente a cada hora. Para disparar uma coleta manual:

```bash
make collect
```

---

## Variaveis de ambiente

Todas as variaveis sao lidas de `.env` na raiz do projeto.

### PostgreSQL

| Variavel | Descricao | Exemplo |
|---|---|---|
| `POSTGRES_HOST` | Host do banco | `localhost` |
| `POSTGRES_PORT` | Porta do banco | `5432` |
| `POSTGRES_DB` | Nome do banco | `smartphones` |
| `POSTGRES_USER` | Usuario do banco | `pipeline_user` |
| `POSTGRES_PASSWORD` | Senha do banco (obrigatoria) | `senha_segura` |

### Redis

| Variavel | Descricao | Exemplo |
|---|---|---|
| `REDIS_HOST` | Host do Redis | `redis` |
| `REDIS_PORT` | Porta do Redis | `6379` |
| `REDIS_STREAM` | Nome do stream principal | `smartphones_raw` |
| `REDIS_FAILED_STREAM` | Stream de mensagens com falha | `smartphones_failed` |

### Amazon BR via RapidAPI

| Variavel | Descricao | Exemplo |
|---|---|---|
| `RAPIDAPI_KEY` | Chave de autenticacao (obrigatoria) | `abc123...` |
| `RAPIDAPI_HOST` | Host da API | `real-time-amazon-data.p.rapidapi.com` |
| `AMAZON_DOMAIN` | Dominio da Amazon | `www.amazon.com.br` |
| `AMAZON_SEARCH_QUERY` | Termo de busca | `smartphone` |
| `AMAZON_MAX_PAGES` | Numero maximo de paginas | `10` |
| `AMAZON_PAGE_SIZE` | Produtos por pagina | `48` |

### Prefect

| Variavel | Descricao | Exemplo |
|---|---|---|
| `PREFECT_API_URL` | URL da API do servidor Prefect | `http://prefect-server:4200/api` |

### Collector

| Variavel | Descricao | Exemplo |
|---|---|---|
| `COLLECTOR_BATCH_SIZE` | Tamanho do lote de publicacao | `50` |
| `COLLECTOR_SLEEP_BETWEEN_PAGES` | Pausa entre paginas (segundos) | `2` |

### Dashboard

| Variavel | Descricao | Exemplo |
|---|---|---|
| `STREAMLIT_PORT` | Porta de exposicao do dashboard | `8501` |

---

## Estrutura do repositorio

```
smartphone-pipeline/
|
|-- collector/                  Coleta Amazon BR via RapidAPI
|   |-- amazon.py               Logica de busca, parsing e paginacao
|   |-- config.py               Carregamento de variaveis de ambiente
|   |-- publisher.py            Publicacao no Redis Stream
|   |-- logger.py               Logger JSON estruturado
|   |-- main.py                 Entry point
|   |-- requirements.txt
|   `-- Dockerfile
|
|-- consumer/                   Leitura do Redis e persistencia no PostgreSQL
|   |-- stream_reader.py        XREADGROUP, XAUTOCLAIM, dead letter
|   |-- persistence.py          INSERT com ON CONFLICT DO NOTHING
|   |-- config.py
|   |-- logger.py
|   |-- main.py
|   |-- requirements.txt
|   `-- Dockerfile
|
|-- dbt/                        Transformacoes SQL em camadas
|   |-- models/
|   |   |-- staging/
|   |   |   `-- stg_products.sql
|   |   `-- marts/
|   |       |-- fct_products.sql
|   |       |-- dim_sellers.sql
|   |       |-- agg_market_overview.sql
|   |       |-- agg_free_shipping.sql
|   |       |-- agg_discount_vs_volume.sql
|   |       |-- agg_condition_distribution.sql
|   |       |-- agg_price_histogram.sql
|   |       |-- agg_price_evolution.sql
|   |       `-- agg_price_variation.sql
|   |-- macros/
|   |   |-- generate_schema_name.sql    Impede prefixo de schema do dbt
|   |   `-- parse_sales_volume.sql      Converte texto para inteiro
|   |-- seeds/
|   |   `-- historical_prices.csv       Dados historicos simulados
|   |-- tests/
|   |   |-- assert_price_positive.sql
|   |   |-- assert_discount_range.sql
|   |   |-- assert_valid_condition.sql
|   |   `-- assert_quantity_positive.sql
|   |-- dbt_project.yml
|   |-- profiles.yml
|   `-- packages.yml
|
|-- orchestration/              Agendamento e execucao do pipeline
|   |-- pipeline.py             Flow Prefect com 6 tasks em sequencia
|   |-- requirements.txt
|   `-- Dockerfile
|
|-- dashboard/                  Visualizacoes Streamlit
|   |-- app.py                  Pagina inicial e KPIs resumidos
|   |-- utils.py                Conexao, cache, formatacao, componentes UI
|   |-- pages/
|   |   |-- 1_visao_geral.py    P1, P2, P6, P8, P9
|   |   |-- 2_vendedores.py     P3, P4, P10
|   |   `-- 3_evolucao.py       P5, P7
|   |-- requirements.txt
|   `-- Dockerfile
|
|-- infra/
|   `-- init.sql                Criacao dos schemas raw, staging, marts
|
|-- docker-compose.yml          Definicao de todos os servicos
|-- Makefile                    Comandos utilitarios
|-- .env.example                Template de configuracao
`-- README.md
```

---

## Camadas do pipeline

### Raw

Schema `raw`. Dados brutos inseridos pelo consumer sem nenhuma transformacao. A tabela `raw.products` armazena o payload JSON original em uma coluna `raw_payload JSONB` alem das colunas normalizadas. Registros duplicados (mesmo `product_id` e `collected_at`) sao ignorados via `ON CONFLICT DO NOTHING`.

### Staging

Schema `staging`. View `stg_products` materializada pelo dbt. Aplica sete transformacoes sobre a camada raw:

1. Deduplicacao por `product_id + collected_at` via `ROW_NUMBER()`
2. Extracao de marca do titulo quando `brand` e nulo (heuristica com 9 marcas)
3. Normalizacao de `condition` para `new` ou `used`
4. Recalculo de `discount_pct` quando ausente ou inconsistente
5. Preenchimento de nulos com `COALESCE`
6. Tipagem explicita de todas as colunas
7. Marcacao de `seed_flag = false` para separar dados reais de seed

### Marts

Schema `marts`. Tabelas analíticas finais, materializadas como `table` ou `incremental`. Prontas para consumo direto pelo dashboard sem joins adicionais.

---

## Modelos dbt

| Modelo | Materializacao | Perguntas respondidas | Descricao |
|---|---|---|---|
| `stg_products` | view | — | Camada de limpeza e tipagem |
| `fct_products` | incremental | P1, P2, P5, P6, P7, P8, P9 | Fato central com todos os produtos |
| `dim_sellers` | table | P3, P10 | Metricas agregadas por marca |
| `agg_market_overview` | table | P1 | Resumo estatístico do mercado |
| `agg_free_shipping` | table | P2, P8 | Proporcao e impacto do frete gratis |
| `agg_discount_vs_volume` | table | P4 | Correlacao desconto x avaliacoes |
| `agg_condition_distribution` | table | P6 | Distribuicao novo x usado |
| `agg_price_histogram` | table | P9 | Histograma em buckets de R$500 |
| `agg_price_evolution` | table | P5 | Serie temporal diaria de precos |
| `agg_price_variation` | table | P7 | Variacao de preco por produto |

---

## Testes de qualidade

O dbt executa onze testes automaticos a cada run de pipeline.

### Testes nativos (schema YAML)

| Teste | Coluna | Modelo |
|---|---|---|
| `not_null` | `product_id` | `stg_products` |
| `not_null` | `price` | `stg_products` |
| `not_null` | `condition` | `stg_products` |
| `not_null` | `collected_at` | `stg_products` |
| `unique` | `product_id + collected_at` | `fct_products` |
| `accepted_values` | `condition` in (`new`, `used`) | `stg_products` |
| `not_null` | `brand` | `dim_sellers` |

### Testes customizados (SQL)

| Arquivo | O que valida |
|---|---|
| `assert_price_positive.sql` | Nenhum produto com preco nulo ou menor ou igual a zero |
| `assert_discount_range.sql` | Desconto sempre entre 0% e 100% |
| `assert_valid_condition.sql` | Condicao somente `new` ou `used`, nunca nula |
| `assert_quantity_positive.sql` | Volume de vendas convertido nao negativo |

Para executar todos os testes:

```bash
make dbt-test
```

Falhas nos testes marcam o flow Prefect como `PARTIAL` (dados preservados, qualidade comprometida) em vez de `FAILED` (perda de dados).

---

## Seed historico

O arquivo `dbt/seeds/historical_prices.csv` contem 35 linhas de dados simulados — 5 produtos ao longo de 7 dias (2026-04-03 a 2026-04-09).

**Por que existe:** as analises de evolucao temporal (P5) e variacao de preco por produto (P7) exigem ao menos dois dias de coleta para serem uteis. Sem o seed, o pipeline funcionaria corretamente mas os graficos de serie temporal estariam vazios durante os primeiros dias de operacao.

**Como identificar dados de seed:** todos os registros do seed possuem a coluna `seed_flag = true`. O dashboard exibe um aviso visual sempre que um periodo contem apenas dados de seed.

**Como carregar:**

```bash
make seed
make dbt-run   # necessario para os marts incorporarem os dados do seed
```

**Como remover:** execute `dbt seed --full-refresh` seguido de `dbt run`. Os dados de seed nao afetam os dados reais — os modelos `agg_*` unem as duas fontes com `UNION ALL` e preservam a coluna `seed_flag` para que o dashboard possa diferenciar.

---

## Comandos Makefile

| Comando | Descricao |
|---|---|
| `make help` | Lista todos os comandos com descricao (padrao) |
| `make up` | Sobe todos os containers em background |
| `make down` | Para todos os containers |
| `make build` | Rebuilda as imagens sem cache |
| `make restart` | Para e reinicia todos os containers |
| `make logs` | Exibe logs de todos os servicos em tempo real |
| `make logs-collector` | Logs apenas do collector |
| `make logs-consumer` | Logs apenas do consumer |
| `make logs-prefect` | Logs do Prefect server e orchestration |
| `make logs-dashboard` | Logs apenas do dashboard |
| `make psql` | Abre o terminal PostgreSQL no container |
| `make db-reset` | Destroi e recria o banco do zero (pede confirmacao) |
| `make db-count` | Conta registros em cada tabela |
| `make collect` | Executa uma coleta manual imediata |
| `make dbt-deps` | Instala dependencias do dbt (dbt_utils) |
| `make dbt-run` | Executa todos os modelos dbt |
| `make dbt-test` | Executa os testes de qualidade |
| `make dbt-docs` | Gera documentacao do dbt |
| `make seed` | Carrega os dados historicos simulados |
| `make test` | Roda todos os testes pytest |
| `make test-collector` | Roda apenas os testes do collector |
| `make health` | Exibe o status de saude de cada servico |
| `make clean` | Remove containers e imagens orfas (preserva volumes) |
| `make reset` | Ambiente completamente do zero (pede confirmacao) |

---

## Perguntas de negócio

| # | Pergunta | Modelo dbt | Pagina do dashboard |
|---|---|---|---|
| P1 | Qual e o preco medio, minimo, maximo e mediano dos smartphones? | `agg_market_overview`, `fct_products` | Visao Geral |
| P2 | Qual a proporcao de produtos com frete gratis? Varia por condicao? | `agg_free_shipping` | Visao Geral |
| P3 | Quais as 10 marcas mais vendidas por volume? | `dim_sellers` | Marcas e Competitividade |
| P4 | Existe correlacao entre desconto e volume de vendas ou avaliacoes? | `agg_discount_vs_volume` | Marcas e Competitividade |
| P5 | Como os precos evoluiram ao longo do tempo? | `agg_price_evolution` | Evolucao de Precos |
| P6 | Como os smartphones se distribuem entre novo e usado? Qual o ticket medio? | `agg_condition_distribution` | Visao Geral |
| P7 | Quais produtos tiveram maior variacao de preco? Quais subiram e caíram? | `agg_price_variation` | Evolucao de Precos |
| P8 | Produtos com frete gratis tem preco medio maior ou menor? | `agg_free_shipping` | Visao Geral |
| P9 | Qual a faixa de preco com maior concentracao de produtos? | `agg_price_histogram` | Visao Geral |
| P10 | Qual marca oferece o melhor custo-beneficio? | `dim_sellers` | Marcas e Competitividade |

---

## Limitacoes conhecidas e recomendacoes para producao

### Limitacoes atuais

**Limite de requisicoes da RapidAPI:** O plano gratuito oferece 100 requisicoes mensais. Com 10 paginas por execucao horaria, o limite e esgotado em menos de 24 horas de operacao continua. Para uso continuo, e necessario um plano pago.

**Brand como proxy de vendedor:** O endpoint de busca da Amazon nao retorna o vendedor individual dos produtos. A marca, extraida heuristicamente do titulo, e usada como substituto nas analises P3 e P10. Os resultados sao uma aproximacao — o mesmo produto pode aparecer vendido por diferentes sellers sob a mesma marca.

**Seed historico como dado simulado:** Os 35 registros historicos do seed foram criados manualmente para viabilizar as analises de serie temporal antes que dados reais de multiplos dias estejam disponíveis. Eles sao identificados por `seed_flag = true` e devem ser desconsiderados em analises que exijam precisao de preco.

### Recomendacoes para producao

| Aspecto | Recomendacao |
|---|---|
| Volume de dados | Assinar o plano pago da RapidAPI ou implementar coleta propria respeitando os termos de uso da Amazon |
| Streaming | Migrar para Apache Kafka quando o volume ultrapassar 10.000 mensagens por hora |
| Orquestracao | Avaliar Apache Airflow se o numero de flows e dependencias entre pipelines crescer significativamente |
| Qualidade de dados | Ampliar os testes dbt com testes de frescor dos dados (`source freshness`) e alertas de anomalia |
| Autenticacao | Adicionar autenticacao ao dashboard Streamlit para ambientes acessíveis externamente |
| Vendedor real | Usar o endpoint `/product-details` da mesma API para obter o seller individual por ASIN |
| Historico real | Desativar o seed apos acumular 7 ou mais dias de coleta real configurando `dbt seed --exclude historical_prices` |
