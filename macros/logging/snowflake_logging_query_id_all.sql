{%- macro snowflake_logging_query_id_all() -%}

{% set results = run_query(
    "
    select 
    QUERY_ID::VARCHAR as QUERY_ID
    ,OPERATOR_STATISTICS:input_rows::integer as input_rows
    from table(get_query_operator_stats(last_query_id()))
    where OPERATOR_TYPE = 'Insert'
    "
) %}

{% set query_id = results.columns[0].values() %}
{% set insert_rows = results.columns[1].values() %}

{{ log('query id: '~ query_id) }}
{{ log('insertet rows: '~ insert_rows) }}

{%- endmacro %}