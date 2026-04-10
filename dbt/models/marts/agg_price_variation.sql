/*
  agg_price_variation — variação de preço entre primeira e última coleta
  por produto, usando dados reais e histórico do seed.

  Para cada produto com ao menos 2 observações temporais distintas, calcula:
    - Primeiro preço registrado
    - Último preço registrado
    - Variação percentual entre os dois
    - Tendência: subiu, caiu ou estável

  Responde a:
    P8 — Como os preços variam ao longo do tempo por produto?
    P3 — Quais produtos tiveram maior desconto ao longo do tempo?
*/

with all_data as (

    select
        product_id,
        title,
        brand,
        price,
        collected_at,
        seed_flag
    from {{ ref('stg_products') }}
    where price > 0

    union all

    select
        product_id,
        title,
        brand,
        price,
        collected_at,
        seed_flag
    from {{ ref('historical_prices') }}
    where price > 0

),

ranked as (

    select
        product_id,
        title,
        brand,
        price,
        collected_at,
        seed_flag,

        row_number() over (
            partition by product_id
            order by collected_at asc
        )                               as rank_asc,

        row_number() over (
            partition by product_id
            order by collected_at desc
        )                               as rank_desc,

        count(*) over (
            partition by product_id
        )                               as num_observations

    from all_data

),

first_last as (

    select
        product_id,
        max(title)                                  as title,
        max(brand)                                  as brand,
        max(num_observations)                       as num_observations,

        -- Primeiro registro
        max(case when rank_asc  = 1 then price        end) as first_price,
        max(case when rank_asc  = 1 then collected_at end) as first_seen_at,

        -- Último registro
        max(case when rank_desc = 1 then price        end) as last_price,
        max(case when rank_desc = 1 then collected_at end) as last_seen_at,

        -- Preço médio e mínimo no período
        round(avg(price), 2)                        as avg_price,
        min(price)                                  as min_price

    from ranked
    group by product_id
    having max(num_observations) >= 2  -- só produtos com histórico real

),

with_variation as (

    select
        product_id,
        title,
        brand,
        num_observations,
        first_seen_at,
        last_seen_at,
        first_price,
        last_price,
        avg_price,
        min_price,

        -- Variação percentual: positivo = preço subiu, negativo = preço caiu
        round(
            (last_price - first_price)
            / nullif(first_price, 0) * 100,
        2)                                          as price_variation_pct,

        -- Desconto máximo em relação ao primeiro preço
        round(
            (first_price - min_price)
            / nullif(first_price, 0) * 100,
        2)                                          as max_drop_pct,

        -- Tendência legível para o dashboard
        case
            when round((last_price - first_price)
                 / nullif(first_price, 0) * 100, 2) > 1
            then 'subiu'
            when round((last_price - first_price)
                 / nullif(first_price, 0) * 100, 2) < -1
            then 'caiu'
            else 'estável'
        end                                         as price_trend

    from first_last

)

select * from with_variation
order by abs(price_variation_pct) desc
