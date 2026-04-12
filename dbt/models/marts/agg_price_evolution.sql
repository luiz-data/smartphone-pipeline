/*
  agg_price_evolution — evolução diária de preços do mercado de smartphones.

  Une dados reais (stg_products) com o seed histórico (historical_prices)
  para que o modelo produza séries temporais completas desde o primeiro acesso,
  mesmo antes de acumular 7+ dias de coletas reais.

  Responde a:
    P8 — Como os preços variam ao longo do tempo?
    P2 — Qual a faixa de preço dos smartphones (por dia)?
    P3 — Evolução dos descontos ao longo do tempo?
*/

with all_data as (

    select
        collected_at,
        brand,
        price,
        original_price,
        discount_pct,
        is_prime,
        is_best_seller,
        free_shipping,
        seed_flag
    from {{ ref('stg_products') }}
    where price > 0

    union all

    select
        collected_at,
        brand,
        price,
        original_price,
        discount_pct,
        is_prime,
        is_best_seller,
        free_shipping,
        seed_flag
    from {{ ref('historical_prices') }}
    where price > 0

),

daily as (

    select
        collected_at::date                          as collection_date,

        -- Volume
        count(*)                                    as total_observations,
        count(distinct
            case when seed_flag = false
            then collected_at::date end
        )                                           as real_data_days,

        -- Preço
        round(avg(price),           2)              as avg_price,
        min(price)                                  as min_price,
        max(price)                                  as max_price,
        round(percentile_cont(0.50)
              within group (order by price)::numeric, 2) as median_price,
        round(percentile_cont(0.25)
              within group (order by price)::numeric, 2) as p25_price,
        round(percentile_cont(0.75)
              within group (order by price)::numeric, 2) as p75_price,

        -- Desconto
        round(avg(discount_pct),    2)              as avg_discount_pct,
        max(discount_pct)                           as max_discount_pct,

        -- Logística e badges do dia
        count(case when free_shipping  then 1 end)  as total_free_shipping,
        count(case when is_prime       then 1 end)  as total_prime,
        count(case when is_best_seller then 1 end)  as total_best_sellers,

        -- Indica se o dia é real ou só histórico
        bool_and(seed_flag)                         as only_seed_data

    from all_data
    group by 1

),

with_variation as (

    select
        *,
        -- Variação percentual do preço médio em relação ao dia anterior
        round(
            (avg_price - lag(avg_price) over (order by collection_date))
            / nullif(lag(avg_price) over (order by collection_date), 0) * 100,
        2)                                          as avg_price_pct_change

    from daily

)

select * from with_variation
order by collection_date
