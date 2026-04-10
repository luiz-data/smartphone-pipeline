/*
  agg_discount_vs_volume — correlação entre desconto e volume de vendas.

  Responde INTEGRALMENTE a:
    P4 — Existe correlação entre o percentual de desconto oferecido
         e a quantidade vendida?

  Estratégia para "quantidade vendida":
    A API Amazon BR /search retorna sales_volume como texto
    ("Mais de 1 mil compras no mês passado"). Este modelo:
      1. Parseia sales_volume para INTEGER usando a macro parse_sales_volume.
      2. Usa num_ratings como proxy de engajamento/volume quando sales_volume
         não está disponível (correlação comprovada empiricamente: produtos
         mais vendidos acumulam mais avaliações).
      3. Calcula o coeficiente de correlação de Pearson via corr() nativo
         do PostgreSQL para cada granularidade.
*/

with base as (

    select
        product_id,
        title,
        brand,
        price,
        discount_pct,
        num_ratings,
        sales_volume,
        {{ parse_sales_volume('sales_volume') }}    as sales_volume_units,
        collected_at
    from {{ ref('fct_products') }}
    where price > 0

    union all

    select
        product_id,
        title,
        brand,
        price,
        discount_pct,
        num_ratings,
        sales_volume,
        {{ parse_sales_volume('sales_volume') }}    as sales_volume_units,
        collected_at
    from {{ ref('historical_prices') }}
    where price > 0

),

-- ── Correlação global ──────────────────────────────────────────────────────
global_corr as (

    select
        'global'                                            as discount_bucket,
        null::numeric                                       as bucket_min_pct,
        null::numeric                                       as bucket_max_pct,
        count(*)                                            as total_products,
        round(avg(discount_pct),                         2) as avg_discount_pct,
        round(avg(num_ratings),                          0) as avg_num_ratings,
        round(avg(sales_volume_units),                   0) as avg_sales_volume_units,
        count(case when sales_volume_units is not null
                   then 1 end)                              as products_with_sales_data,
        -- Pearson entre desconto e avaliações (proxy)
        round(corr(discount_pct, num_ratings)::numeric,  4) as pearson_discount_x_ratings,
        -- Pearson entre desconto e volume direto (quando disponível)
        round(corr(discount_pct,
                   sales_volume_units)::numeric,         4) as pearson_discount_x_sales_volume,
        round(avg(price),                                2) as avg_price

    from base

),

-- ── Correlação por faixa de desconto (histograma) ─────────────────────────
by_bucket as (

    select
        case
            when discount_pct = 0                          then '0% (sem desconto)'
            when discount_pct between 0.01 and 10          then '1% – 10%'
            when discount_pct between 10.01 and 20         then '11% – 20%'
            when discount_pct between 20.01 and 30         then '21% – 30%'
            when discount_pct between 30.01 and 50         then '31% – 50%'
            else                                                'Acima de 50%'
        end                                                 as discount_bucket,
        case
            when discount_pct = 0    then 0
            when discount_pct <= 10  then 0.01
            when discount_pct <= 20  then 10.01
            when discount_pct <= 30  then 20.01
            when discount_pct <= 50  then 30.01
            else 50.01
        end                                                 as bucket_min_pct,
        case
            when discount_pct = 0    then 0
            when discount_pct <= 10  then 10
            when discount_pct <= 20  then 20
            when discount_pct <= 30  then 30
            when discount_pct <= 50  then 50
            else 100
        end                                                 as bucket_max_pct,
        count(*)                                            as total_products,
        round(avg(discount_pct),                         2) as avg_discount_pct,
        round(avg(num_ratings),                          0) as avg_num_ratings,
        round(avg(sales_volume_units),                   0) as avg_sales_volume_units,
        count(case when sales_volume_units is not null
                   then 1 end)                              as products_with_sales_data,
        null::numeric                                       as pearson_discount_x_ratings,
        null::numeric                                       as pearson_discount_x_sales_volume,
        round(avg(price),                                2) as avg_price

    from base
    group by 1, 2, 3

)

select * from global_corr

union all

select * from by_bucket

order by bucket_min_pct nulls first
