-- assert_price_positive
-- Falha se qualquer produto tiver price <= 0 ou price IS NULL.
-- Preço zero ou negativo indica problema no parser do collector ou dado corrompido.
-- O teste passa quando esta query retorna 0 linhas.

select
    product_id,
    title,
    price,
    collected_at,
    source
from {{ ref('stg_products') }}
where price is null
   or price <= 0
