/*
  dim_sellers — dimensão de vendedores/marcas com métricas completas.

  Nota: a API de busca da Amazon BR (/search via RapidAPI) não retorna
  seller_id/seller_name. Esta dimensão usa `brand` como proxy de vendedor,
  que é a granularidade analítica disponível nesta fonte.
  Informações de seller específico estariam disponíveis via /product-details.

  Unifica dados reais (fct_products) e históricos (seed historical_prices)
  para disponibilidade imediata.

  Responde INTEGRALMENTE a:
    P3  — Quais são os 10 vendedores (marcas) com maior volume de vendas?
    P10 — Quais vendedores têm o melhor equilíbrio entre preço e volume?

  Contribui para:
    P1  — Preço por segmento de marca
    P2  — Proporção de frete grátis por marca
*/

with all_data as (

    select
        product_id,
        brand,
        price,
        original_price,
        discount_pct,
        rating,
        num_ratings,
        free_shipping,
        is_prime,
        is_best_seller,
        is_amazon_choice,
        sales_volume,
        {{ parse_sales_volume('sales_volume') }}    as sales_volume_units,
        collected_at
    from {{ ref('fct_products') }}
    where price > 0

    union all

    select
        product_id,
        brand,
        price,
        original_price,
        discount_pct,
        rating,
        num_ratings,
        free_shipping,
        is_prime,
        is_best_seller,
        is_amazon_choice,
        sales_volume,
        {{ parse_sales_volume('sales_volume') }}    as sales_volume_units,
        collected_at
    from {{ ref('historical_prices') }}
    where price > 0

),

brand_metrics as (

    select
        coalesce(brand, 'Sem marca identificada')       as brand,

        -- ── Volume (P3) ───────────────────────────────────────────────────
        count(distinct product_id)                      as total_products,
        count(*)                                        as total_observations,

        -- Volume de vendas parseado do texto da Amazon
        sum(sales_volume_units)                         as total_sales_volume_units,
        round(avg(sales_volume_units), 0)               as avg_sales_volume_units,

        -- num_ratings como proxy de engajamento/volume de vendas
        sum(num_ratings)                                as total_reviews,
        round(avg(num_ratings), 0)                      as avg_num_ratings,

        -- ── Preço ─────────────────────────────────────────────────────────
        round(avg(price),          2)                   as avg_price,
        min(price)                                      as min_price,
        max(price)                                      as max_price,
        round(stddev(price),       2)                   as stddev_price,

        -- Desconto
        round(avg(discount_pct),   2)                   as avg_discount_pct,
        max(discount_pct)                               as max_discount_pct,

        -- Avaliações
        round(avg(rating),         2)                   as avg_rating,
        max(rating)                                     as max_rating,

        -- Logística e badges
        count(case when free_shipping    then 1 end)    as total_free_shipping,
        count(case when is_prime         then 1 end)    as total_prime,
        count(case when is_best_seller   then 1 end)    as total_best_sellers,
        count(case when is_amazon_choice then 1 end)    as total_amazon_choice,
        round(
            count(case when free_shipping then 1 end)::numeric
            / nullif(count(*), 0) * 100,
        2)                                              as pct_free_shipping,

        -- Janela temporal
        min(collected_at)                               as first_seen_at,
        max(collected_at)                               as last_seen_at

    from all_data
    group by 1

),

with_scores as (

    select
        *,

        -- ── Ranking por volume de vendas (P3) ────────────────────────────
        -- Prioriza sales_volume_units (direto da Amazon); usa total_reviews
        -- como fallback quando a Amazon não exibe volume de vendas.
        rank() over (
            order by coalesce(total_sales_volume_units, total_reviews) desc
        )                                               as rank_by_sales_volume,

        -- ── Score de competitividade (P10) ────────────────────────────────
        -- Fórmula: (avg_rating × total_reviews) / avg_price
        -- Premia marcas com boa avaliação, muitas reviews e preço competitivo.
        -- Quanto maior o score, melhor o equilíbrio preço × volume.
        case
            when avg_price > 0 and avg_rating > 0
            then round(
                (avg_rating * total_reviews) / avg_price,
            4)
            else null
        end                                             as competitiveness_score,

        rank() over (
            order by
                case
                    when avg_price > 0 and avg_rating > 0
                    then (avg_rating * total_reviews) / avg_price
                    else null
                end desc nulls last
        )                                               as rank_by_competitiveness

    from brand_metrics

)

select * from with_scores
order by rank_by_sales_volume
