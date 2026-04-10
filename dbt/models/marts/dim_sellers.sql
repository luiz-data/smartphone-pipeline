/*
  dim_sellers — dimensão de "vendedores" agrupada por marca.

  Nota: a API de busca da Amazon BR (/search via RapidAPI) não retorna
  dados de seller individual. Esta dimensão usa `brand` como proxy de
  vendedor/fabricante, que é a granularidade analítica disponível.
  Informações de seller específico estariam disponíveis via /product-details.

  Unifica dados reais (stg_products) e históricos (seed historical_prices)
  para que a dimensão funcione desde o primeiro acesso.

  Responde a:
    P1 — Quais marcas dominam o mercado?
    P2 — Qual a faixa de preço por marca?
    P4 — Marcas com melhores avaliações?
*/

with all_data as (

    select * from {{ ref('stg_products') }}

    union all

    -- Inclui dados históricos do seed para que a dimensão tenha conteúdo
    -- mesmo antes da primeira coleta real completar 7 dias.
    select * from {{ ref('historical_prices') }}

),

brand_metrics as (

    select
        coalesce(brand, 'Sem marca identificada')       as brand,

        -- Volume
        count(distinct product_id)                      as total_products,
        count(*)                                        as total_observations,

        -- Preço
        round(avg(price),          2)                   as avg_price,
        min(price)                                      as min_price,
        max(price)                                      as max_price,
        round(stddev(price),       2)                   as stddev_price,

        -- Desconto
        round(avg(discount_pct),   2)                   as avg_discount_pct,
        max(discount_pct)                               as max_discount_pct,

        -- Avaliações
        round(avg(rating),         2)                   as avg_rating,
        sum(num_ratings)                                as total_reviews,
        max(rating)                                     as max_rating,

        -- Logística e badges
        count(case when free_shipping    then 1 end)    as total_free_shipping,
        count(case when is_prime         then 1 end)    as total_prime,
        count(case when is_best_seller   then 1 end)    as total_best_sellers,
        count(case when is_amazon_choice then 1 end)    as total_amazon_choice,

        -- Percentual frete grátis
        round(
            count(case when free_shipping then 1 end)::numeric
            / nullif(count(*), 0) * 100,
        2)                                              as pct_free_shipping,

        -- Janela temporal dos dados
        min(collected_at)                               as first_seen_at,
        max(collected_at)                               as last_seen_at

    from all_data
    where price > 0

    group by 1

)

select
    brand,
    total_products,
    total_observations,
    avg_price,
    min_price,
    max_price,
    stddev_price,
    avg_discount_pct,
    max_discount_pct,
    avg_rating,
    total_reviews,
    max_rating,
    total_free_shipping,
    total_prime,
    total_best_sellers,
    total_amazon_choice,
    pct_free_shipping,
    first_seen_at,
    last_seen_at

from brand_metrics
order by total_products desc
