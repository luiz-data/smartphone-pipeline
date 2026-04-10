/*
  agg_price_histogram — histograma de preços em faixas de R$500.

  Responde INTEGRALMENTE a:
    P9 — Qual a faixa de preço com maior concentração de produtos
         (histograma em faixas de R$ 500)?

  Método: floor(price / 500) * 500 agrupa todos os preços em múltiplos de 500.
  Exemplo:
    price = 1.250 → floor(1250/500)*500 = 1000 → bucket "R$1.000 – R$1.499"
    price = 672   → floor(672/500)*500  = 500  → bucket "R$500 – R$999"

  Inclui seed histórico para ter distribuição desde o primeiro acesso.
  Cap de R$15.000 para evitar que outliers criem buckets isolados.
*/

with all_data as (

    select product_id, price
    from {{ ref('fct_products') }}
    where price > 0 and price <= 15000

    union all

    select product_id, price
    from {{ ref('historical_prices') }}
    where price > 0 and price <= 15000

),

bucketed as (

    select
        (floor(price / 500) * 500)::integer             as bucket_start,
        (floor(price / 500) * 500 + 499)::integer        as bucket_end,
        price,
        product_id
    from all_data

),

histogram as (

    select
        bucket_start,
        bucket_end,
        -- Label legível para o eixo X do histograma
        'R$' || to_char(bucket_start, 'FM999G999')
        || ' – R$' || to_char(bucket_end, 'FM999G999') as price_range_label,

        count(distinct product_id)                       as total_distinct_products,
        count(*)                                         as total_observations,
        round(avg(price), 2)                             as avg_price_in_bucket,
        min(price)                                       as min_price_in_bucket,
        max(price)                                       as max_price_in_bucket

    from bucketed
    group by bucket_start, bucket_end

)

select
    bucket_start,
    bucket_end,
    price_range_label,
    total_distinct_products,
    total_observations,
    avg_price_in_bucket,
    min_price_in_bucket,
    max_price_in_bucket,
    -- Percentual em relação ao total — identifica o bucket mais concentrado
    round(
        total_observations::numeric
        / sum(total_observations) over () * 100,
    2)                                                   as pct_of_total,
    -- Ranking: 1 = faixa com mais produtos
    rank() over (order by total_observations desc)       as concentration_rank

from histogram
order by bucket_start
