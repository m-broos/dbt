{#
-------------------------------------------------------------------------------------
Macro Name   : this_exists
Description  : Checks if a dbt relation (table/view) exists in the database.
               
               - Without parameters: Checks if the current model's table exists
               - With table_name parameter: Checks if a specific table exists using get_static_table

Arguments:
    table_name (string, optional) : Name of table to check. If not provided, checks current 'this' relation
    prod_schema (string, optional): Schema to use for static table lookup (default: 'edw')

Returns:
    boolean: True if the relation exists, otherwise False

-------------------------------------------------------------------------------------
Example Usage:

    1. Check current model's table:
    {% if this_exists() %}
      -- Logic when current table exists
    {% else %}
      -- Fallback (e.g. dummy-table or initialization)
    {% endif %}

    2. Check specific table by name:
    {% if this_exists('f_co_bon_position') %}
      -- Logic when f_co_bon_position exists
    {% else %}
      -- Fallback logic
    {% endif %}

    3. Check specific table with custom schema:
    {% if this_exists('f_co_bon_position', 'dma') %}
      -- Logic when f_co_bon_position exists in dma schema
    {% endif %}
-------------------------------------------------------------------------------------
#}

{% macro this_exists(table_name=none, prod_schema='edw') %}
  
  {%- if table_name is none -%}
    {# Check current 'this' relation #}
    {%- set rel = adapter.get_relation(
          database=this.database,
          schema=this.schema,
          identifier=this.name
    ) -%}
  {%- else -%}
    {# Check specific table using get_static_table #}
    {%- set full_table_name = get_static_table(table_name, prod_schema) -%}
    {%- set table_parts = full_table_name.split('.') -%}
    {%- set target_database = table_parts[0] -%}
    {%- set target_schema = table_parts[1] -%}
    {%- set target_identifier = table_parts[2] -%}
    
    {%- set rel = adapter.get_relation(
          database=target_database,
          schema=target_schema,
          identifier=target_identifier
    ) -%}
  {%- endif -%}
  
  {{ return(rel is not none) }}
{% endmacro %}