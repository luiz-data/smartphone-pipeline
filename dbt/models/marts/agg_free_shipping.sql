/*
  agg_free_shipping — análise de frete grátis por condição e comparação de preço.

  Responde INTEGRALMENTE a:
    P2 — Qual proporção dos produtos oferece frete grátis?
         Isso varia entre produtos novos e usados?
    P8 — Os produtos com frete grátis têm preço médio maior
         ou menor do que os sem frete grátis?

  Nota sobre a fonte Amazon BR:
    A busca /search retorna predominantemente produtos 'new'.
    Se no futuro outras fontes adicionarem produtos 'used', este modelo
    já está preparado para mostrar a variação por condição.
*/

with base as (

    select
        price,
        free_shipping,
        condition,
        discount_pct
    from {{ ref('fct_products') }}
    where price > 0

    union all

    select
        price,
        free_shipping,
        condition,
        discount_pct
    from {{ ref('historical_prices') }}
    where price > 0

),

-- ── P2: Proporção de frete grátis, quebrada por condition ─────────────────
by_condition as (

    select
        condition,
        free_shipping,
        count(*)                                            as total_products,
        round(avg(price),        2)                         as avg_price,
        min(price)                                          as min_price,
        max(price)                                          as max_price,
        round(avg(discount_pct), 2)                         as avg_discount_pct,
        sum(count(*)) over (partition by condition)         as condition_total
    from base
    group by condition, free_shipping

),

proportion as (

    select
        condition,
        free_shipping,
        total_products,
        round(total_products::numeric / condition_total * 100, 2) as pct_within_condition,
        avg_price,
        min_price,
        max_price,
        avg_discount_pct
    from by_condition

),

-- ── P8: Preço médio com vs sem frete grátis (visão global) ───────────────
global_comparison as (

    select
        free_shipping,
        count(*)                    as total_products,
        round(avg(price),        2) as avg_price,
        min(price)                  as min_price,
        max(price)                  as max_price,
        round(avg(discount_pct), 2) as avg_discount_pct
    from base
    group by free_shipping

),

global_with_pct as (

    select
        free_shipping,
        total_products,
        round(total_products::numeric / sum(total_products) over () * 100, 2) as pct_of_total,
        avg_price,
        min_price,
        max_price,
        avg_discount_pct,
        -- Diferença de preço médio em relação ao grupo sem frete grátis
        round(
            avg_price - max(case when not free_shipping then avg_price end) over (),
        2)                          as price_diff_vs_no_free_shipping

    from global_comparison

)

-- Entrega as duas visões em tabela única, com escopo identificado
select
    'por_condition'                 as scope,
    condition,
    case when free_shipping
         then 'com frete grátis'
         else 'sem frete grátis'
    end                             as shipping_group,
    total_products,
    pct_within_condition            as pct,
    avg_price,
    min_price,
    max_price,
    avg_discount_pct,
    null::numeric                   as price_diff_vs_no_free_shipping
from proportion

union all

select
    'global'                        as scope,
    'todos'                         as condition,
    case when free_shipping
         then 'com frete grátis'
         else 'sem frete grátis'
    end                             as shipping_group,
    total_products,
    pct_of_total                    as pct,
    avg_price,
    min_price,
    max_price,
    avg_discount_pct,
    price_diff_vs_no_free_shipping
from global_with_pct

order by scope, condition, shipping_group
