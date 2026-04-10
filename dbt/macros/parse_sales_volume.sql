-- parse_sales_volume(col)
-- Converte textos de volume de vendas da Amazon BR para INTEGER aproximado.
--
-- Exemplos tratados:
--   "Mais de 1 mil compras no mês passado"    → 1000
--   "Mais de 1,5 mil compras no mês passado"  → 1500
--   "Mais de 500 compras no mês passado"       → 500
--   ""  / NULL                                 → NULL
--
-- Estratégia (ordem de prioridade):
--   1. Decimal + "mil"  → "1,5 mil" → concat('1','.','5')::numeric * 1000
--   2. Inteiro + "mil"  → "2 mil"   → 2 * 1000
--   3. Só número        → "500"     → 500
--   4. Demais           → NULL

{% macro parse_sales_volume(col) %}
    case
        when {{ col }} ~* '\d+[,.]\d+\s*mil'
        then (
            concat(
                (regexp_match({{ col }}, '(\d+)[,.](\d+)\s*mil'))[1],
                '.',
                (regexp_match({{ col }}, '(\d+)[,.](\d+)\s*mil'))[2]
            )::numeric * 1000
        )::integer

        when {{ col }} ~* '\d+\s*mil'
        then (
            (regexp_match({{ col }}, '(\d+)\s*mil'))[1]::numeric * 1000
        )::integer

        when {{ col }} ~ '\d+'
        then nullif(regexp_replace({{ col }}, '[^0-9]', '', 'g'), '')::integer

        else null
    end
{% endmacro %}
