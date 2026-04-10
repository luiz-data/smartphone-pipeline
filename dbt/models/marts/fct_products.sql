/*
  fct_products — tabela fato. Um registro por produto por coleta.

  Materialização incremental com estratégia merge:
    - unique_key: [product_id, collected_at]
    - Em cada run, apenas os novos registros são inseridos ou atualizados.
    - Não contém dados do seed histórico (seed_flag = false).

  Responde a:
    P2 — Faixa de preço dos smartphones
    P3 — Produtos com maior desconto
    P4 — Mais bem avaliados
    P6 — Frete grátis e preço
    P7 — Prime / Amazon Choice
    P8 — Variação de preço ao longo do tempo
    P9 — Relação avaliação × preço
    P10 — Melhor custo-benefício
*/

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['product_id', 'collected_at']
    )
}}

with staged as (

    select * from {{ ref('stg_products') }}
    where seed_flag = false   -- apenas dados reais na tabela fato

),

enriched as (

    select
        -- ── Chaves ───────────────────────────────────────────────────────
        ingestion_id,
        batch_id,
        source,
        collected_at,
        inserted_at,
        collected_at::date                          as collection_date,

        -- ── Produto ───────────────────────────────────────────────────────
        product_id,
        title,
        brand,
        condition,
        url,
        thumbnail_url,

        -- ── Preço ─────────────────────────────────────────────────────────
        price,
        original_price,
        discount_pct,
        currency,

        -- Faixa de preço (P2): categoriza o produto por valor
        case
            when price < 1000              then 'Até R$1.000'
            when price < 2000              then 'R$1.000 – R$2.000'
            when price < 3500              then 'R$2.000 – R$3.500'
            when price < 5000              then 'R$3.500 – R$5.000'
            else                                'Acima de R$5.000'
        end                                         as price_bucket,

        -- Score de custo-benefício (P10): normaliza rating pelo preço
        -- Quanto maior o score, melhor a relação avaliação/custo.
        case
            when price > 0 and rating > 0
            then round(rating * num_ratings / price, 4)
            else null
        end                                         as value_score,

        -- ── Avaliações ────────────────────────────────────────────────────
        rating,
        num_ratings,

        -- ── Badges Amazon ─────────────────────────────────────────────────
        is_best_seller,
        is_amazon_choice,
        is_prime,

        -- ── Logística ─────────────────────────────────────────────────────
        free_shipping,
        delivery_text,

        -- ── Volume e ofertas ──────────────────────────────────────────────
        sales_volume,
        num_offers,

        -- Flag
        seed_flag

    from staged

)

select * from enriched

{% if is_incremental() %}
-- Incremental: só processa linhas com collected_at posterior ao máximo já carregado
where collected_at > (select max(collected_at) from {{ this }})
{% endif %}
