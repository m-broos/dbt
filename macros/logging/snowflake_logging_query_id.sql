{%- macro snowflake_logging_query_id() -%}

--execute last_query query to get lst_query_id
{% set results = run_query('SELECT last_query_id() AS last_query_id') %}
--get result
{% set results_list = results.columns[0].values() %}
--output last query into the logs
{{ log('snowflake_query_id: '~ results_list) }}

{%- endmacro %}