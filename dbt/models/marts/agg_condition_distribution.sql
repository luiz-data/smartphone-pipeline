/*
  agg_condition_distribution — distribuição de produtos por condição (novo vs. usado).

  Responde INTEGRALMENTE a:
    P6 — Qual a distribuição dos produtos por condição (novo vs. usado)
         e qual grupo tem ticket médio mais alto?

  Nota sobre a fonte Amazon BR:
    A busca /search retorna predominantemente produtos 'new'. A distribuição
    atual reflete essa característica da fonte. O modelo está preparado para
    acomodar produtos 'used' caso sejam adicionados via outra fonte ou via
    endpoint /product-details.

  Inclui seed histórico para garantir dados desde o primeiro acesso.
*/

with all_data as (

    select
        product_id,
        condition,
        price,
        original_price,
        discount_pct,
        free_shipping,
        rating,
        num_ratings
    from {{ ref('fct_products') }}
    where price > 0

    union all

    select
        product_id,
        condition,
        price,
        original_price,
        discount_pct,
        free_shipping,
        rating,
        num_ratings
    from {{ ref('historical_prices') }}
    where price > 0

),

by_condition as (

    select
        condition,

        -- Volume
        count(distinct product_id)                              as total_distinct_products,
        count(*)                                                as total_observations,
        round(
            count(*)::numeric / sum(count(*)) over () * 100,
        2)                                                      as pct_of_total,

        -- ── Ticket médio (resposta direta à P6) ──────────────────────────
        round(avg(price),            2)                         as avg_price,
        min(price)                                              as min_price,
        max(price)                                              as max_price,
        round(percentile_cont(0.50)
              within group (order by price), 2)                 as median_price,

        -- Desconto
        round(avg(discount_pct),     2)                         as avg_discount_pct,
        count(case when discount_pct > 0 then 1 end)            as products_with_discount,

        -- Logística
        count(case when free_shipping then 1 end)               as total_free_shipping,
        round(
            count(case when free_shipping then 1 end)::numeric
            / nullif(count(*), 0) * 100,
        2)                                                      as pct_free_shipping,

        -- Avaliações
        round(avg(rating),           2)                         as avg_rating,
        round(avg(num_ratings),      0)                         as avg_num_ratings

    from all_data
    group by condition

)

select
    *,
    -- Ranking por ticket médio: identifica qual condição tem preço mais alto
    rank() over (order by avg_price desc)                       as rank_by_avg_price

from by_condition
order by total_observations desc
