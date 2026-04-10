-- assert_quantity_positive
-- Falha se num_ratings for negativo.
-- Na Amazon BR, num_ratings representa o total de avaliações recebidas pelo
-- produto — um valor negativo é impossível e indica corrupção de dado.
-- O teste passa quando esta query retorna 0 linhas.
--
-- Nota: sold_quantity não existe na fonte Amazon BR. num_ratings é o proxy
-- de engajamento/volume equivalente disponível nesta fonte de dados.

select
    product_id,
    title,
    num_ratings,
    collected_at
from {{ ref('stg_products') }}
where num_ratings < 0
