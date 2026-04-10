-- assert_valid_condition
-- Falha se condition for nulo ou contiver valor fora do conjunto {'new', 'used'}.
-- A camada de staging normaliza condition para esses dois valores;
-- qualquer outro valor indica falha na transformação.
-- O teste passa quando esta query retorna 0 linhas.

select
    product_id,
    title,
    condition,
    collected_at
from {{ ref('stg_products') }}
where condition is null
   or condition not in ('new', 'used')
