/*
  agg_market_overview — visão consolidada do mercado de smartphones.

  Une fct_products (dados reais) com historical_prices (seed) para que o
  modelo tenha conteúdo desde o primeiro acesso.

  Produz UMA linha com os indicadores globais do catálogo coletado.

  Responde INTEGRALMENTE a:
    P1 — Qual é o preço médio, mínimo e máximo de smartphones na plataforma?
*/

with all_data as (

    select
        product_id,
        price,
        original_price,
        discount_pct,
        rating,
        num_ratings,
        free_shipping,
        condition,
        is_prime,
        is_best_seller,
        is_amazon_choice,
        collected_at
    from {{ ref('fct_products') }}
    where price > 0

    union all

    select
        product_id,
        price,
        original_price,
        discount_pct,
        rating,
        num_ratings,
        free_shipping,
        condition,
        is_prime,
        is_best_seller,
        is_amazon_choice,
        collected_at
    from {{ ref('historical_prices') }}
    where price > 0

)

select
    -- ── Cobertura temporal ────────────────────────────────────────────────
    min(collected_at)                                       as first_collected_at,
    max(collected_at)                                       as last_collected_at,
    count(distinct collected_at::date)                      as total_collection_days,

    -- ── Volume de catálogo ────────────────────────────────────────────────
    count(distinct product_id)                              as total_distinct_products,
    count(*)                                                as total_observations,

    -- ── P1: Preço médio, mínimo e máximo ─────────────────────────────────
    round(avg(price),                                    2) as avg_price,
    min(price)                                              as min_price,
    max(price)                                              as max_price,
    round(percentile_cont(0.50)
          within group (order by price),                 2) as median_price,
    round(percentile_cont(0.25)
          within group (order by price),                 2) as p25_price,
    round(percentile_cont(0.75)
          within group (order by price),                 2) as p75_price,

    -- ── Desconto ─────────────────────────────────────────────────────────
    round(avg(discount_pct),                             2) as avg_discount_pct,
    max(discount_pct)                                       as max_discount_pct,
    count(case when discount_pct > 0 then 1 end)            as products_with_discount,

    -- ── Avaliações ────────────────────────────────────────────────────────
    round(avg(rating),                                   2) as avg_rating,
    round(avg(num_ratings),                              0) as avg_num_ratings,

    -- ── Logística ─────────────────────────────────────────────────────────
    count(case when free_shipping then 1 end)               as total_free_shipping,
    round(
        count(case when free_shipping then 1 end)::numeric
        / nullif(count(*), 0) * 100,
    2)                                                      as pct_free_shipping,

    -- ── Badges ────────────────────────────────────────────────────────────
    count(case when is_prime         then 1 end)            as total_prime,
    count(case when is_best_seller   then 1 end)            as total_best_sellers,
    count(case when is_amazon_choice then 1 end)            as total_amazon_choice

from all_data
