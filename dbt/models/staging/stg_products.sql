/*
  stg_products — camada de limpeza e tipagem sobre raw.products.

  Responsabilidades:
    1. Lê de raw.products (fonte Amazon BR via RapidAPI).
    2. Deduplica por product_id + collected_at mantendo a linha mais recente.
    3. Extrai marca do título quando brand é nulo (heurística por prefixo).
    4. Padroniza condition para 'new' | 'used'.
    5. Garante tipos corretos (NUMERIC, BOOLEAN, TIMESTAMPTZ).
    6. Recalcula discount_pct quando ausente ou inconsistente.
    7. Preenche nulos com valores neutros (COALESCE).
    8. Expõe raw_payload JSONB para acesso a campos não normalizados.
    9. Marca seed_flag = false para distinguir de dados históricos simulados.
*/

with source as (

    select * from {{ source('raw', 'products') }}

),

deduped as (

    -- Garante unicidade por product_id + collected_at antes de qualquer
    -- transformação. Mantém a linha com inserted_at mais recente caso
    -- existam duplicatas na origem (não deve ocorrer graças ao UNIQUE
    -- constraint, mas é uma defesa extra).
    select
        *,
        row_number() over (
            partition by product_id, collected_at
            order by inserted_at desc
        ) as _row_num

    from source
    where product_id is not null

),

cleaned as (

    select
        -- ── Metadados de ingestão ─────────────────────────────────────────
        ingestion_id,
        batch_id,
        'amazon_br'::text                               as source,
        collected_at::timestamptz                       as collected_at,
        inserted_at::timestamptz                        as inserted_at,

        -- ── Identificação do produto ──────────────────────────────────────
        product_id,
        title,

        -- Extrai marca do título quando brand não foi preenchido pelo collector.
        -- A busca /search da Amazon BR não retorna brand; a heurística cobre
        -- os principais fabricantes do mercado BR.
        coalesce(
            nullif(brand, ''),
            case
                when title ilike '%Samsung%'                 then 'Samsung'
                when title ilike '%iPhone%'
                  or title ilike 'Apple%'                   then 'Apple'
                when title ilike 'Motorola%'
                  or title ilike '%Moto %'                  then 'Motorola'
                when title ilike 'Xiaomi%'
                  or title ilike '%Redmi%'
                  or title ilike '%POCO%'                   then 'Xiaomi'
                when title ilike 'Realme%'                  then 'Realme'
                when title ilike 'LG %'
                  or title ilike 'Smartphone LG%'           then 'LG'
                when title ilike 'Sony%'                    then 'Sony'
                when title ilike 'Asus%'
                  or title ilike '%ZenFone%'                then 'Asus'
                when title ilike 'Positivo%'                then 'Positivo'
                else null
            end
        )                                               as brand,

        -- Normaliza condition: qualquer variante de "usado/used/refurbished"
        -- vira 'used'; todo o resto (incluindo nulo) vira 'new'.
        case
            when lower(coalesce(condition, 'new')) in ('usado', 'used', 'refurbished')
            then 'used'
            else 'new'
        end                                             as condition,

        url,
        thumbnail_url,

        -- ── Preço ─────────────────────────────────────────────────────────
        price::numeric                                  as price,
        original_price::numeric                         as original_price,

        -- Recalcula discount_pct quando está ausente ou inconsistente
        -- (ex.: original_price > price mas discount_pct = 0).
        case
            when original_price > 0
             and price is not null
             and price < original_price
            then round((original_price - price) / original_price * 100, 2)
            else coalesce(discount_pct, 0.0)
        end                                             as discount_pct,

        coalesce(currency, 'BRL')                       as currency,

        -- ── Avaliações ────────────────────────────────────────────────────
        rating::numeric                                 as rating,
        coalesce(num_ratings, 0)                        as num_ratings,

        -- ── Badges Amazon ─────────────────────────────────────────────────
        coalesce(is_best_seller,   false)               as is_best_seller,
        coalesce(is_amazon_choice, false)               as is_amazon_choice,
        coalesce(is_prime,         false)               as is_prime,

        -- ── Logística ─────────────────────────────────────────────────────
        coalesce(free_shipping, false)                  as free_shipping,
        coalesce(delivery_text, '')                     as delivery_text,

        -- ── Volume e ofertas ──────────────────────────────────────────────
        -- raw_payload como fallback caso a coluna normalizada esteja nula
        coalesce(
            nullif(sales_volume, ''),
            raw_payload->>'sales_volume'
        )                                               as sales_volume,
        coalesce(num_offers, 0)                         as num_offers,

        -- Acesso direto ao JSONB para campos não normalizados
        raw_payload,

        -- Flag que distingue dados reais de dados do seed histórico
        false                                           as seed_flag

    from deduped
    -- Exclui produtos sem preço: não têm valor analítico e violam constraints
    -- downstream. Produtos recondicionados da Amazon BR frequentemente chegam
    -- sem preço via endpoint /search.
    where _row_num = 1
      and price is not null
      and price::numeric > 0

)

select * from cleaned
