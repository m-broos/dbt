{% macro generate_valid_to(partition_by, valid_from) -%}
  {# Wenn partition_by eine Liste ist, die Spalten mit Komma joinen #}
  {%- if partition_by is iterable and partition_by is not string -%}
    {%- set partitions = partition_by | join(', ') -%}
  {%- else -%}
    {%- set partitions = partition_by -%}
  {%- endif -%}

COALESCE(
  LEAD({{ valid_from }}) OVER (
    PARTITION BY {{ partitions }}
    ORDER   BY {{ valid_from }}
  ) - INTERVAL '1 DAY',
  DATE '9999-12-31'
) 
{%- endmacro %}
