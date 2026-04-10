-- assert_discount_range
-- Falha se discount_pct estiver fora do intervalo [0, 100].
-- Desconto negativo (preço final > original) ou acima de 100% são impossíveis
-- e indicam erro de cálculo ou dado inválido da API.
-- O teste passa quando esta query retorna 0 linhas.

select
    product_id,
    title,
    price,
    original_price,
    discount_pct,
    collected_at
from {{ ref('stg_products') }}
where discount_pct is not null
  and (
      discount_pct < 0
   or discount_pct > 100
  )
