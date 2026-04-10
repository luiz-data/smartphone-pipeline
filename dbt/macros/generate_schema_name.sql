-- Sobrescreve o comportamento padrão do dbt, que prefixa o schema alvo
-- (ex.: "dev") ao custom_schema_name dos modelos, resultando em "dev_staging".
--
-- Com esta macro, +schema: staging no dbt_project.yml cria objetos diretamente
-- no schema "staging" — alinhado aos schemas raw/staging/marts já criados pelo
-- infra/init.sql.
--
-- Comportamento:
--   custom_schema_name = None  → usa target.schema (padrão de conexão)
--   custom_schema_name = "staging" → usa "staging" literalmente

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
