{#
-----------------------------------------------------------------------------------
Macro: delete_if_incremental

Description:
This macro deletes all rows from a target model table if the current run mode
is incremental. It is intended to be used as a `pre_hook` in a dbt model that
uses `materialized='incremental'`, but where a full table refresh is still
desired without changing the materialization.

This is useful for cases where you want to:
- Leverage incremental materialization for flexibility
- Retain table structure or permissions
- Simulate full refresh behavior for staging or comparison models

Parameters:
- model (dict): The `this` variable from dbt, which contains the current model's
  database, schema, and identifier

Returns:
- A SQL `DELETE FROM` command during an incremental run
- A dummy SQL statement during compilation or non-incremental runs

Usage Example:
---------------------------------------------------------------
{{ config(
    materialized='incremental',
    pre_hook=[
        "{{ delete_if_incremental(this) }}"
    ]
) }}
-----------------------------------------------------------------------------------
#}

{% macro delete_if_incremental(model) %}
    {% if execute and is_incremental() %}
        {% set full_table_name = model.database ~ '.' ~ model.schema ~ '.' ~ model.identifier %}
        {% do log("Incremental mode detected. Deleting all records from: " ~ full_table_name, info=True) %}
        delete from {{ full_table_name }};
    {% else %}
        select 'no delete required' as dummy;
    {% endif %}
{% endmacro %}
