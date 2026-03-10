{% macro generate_valid_version(partition_by, valid_from) -%}
  {# mehrere Partition-Spalten joinen, falls eine Liste übergeben wird #}
  {%- if partition_by is iterable and partition_by is not string -%}
    {%- set partitions = partition_by | join(', ') -%}
  {%- else -%}
    {%- set partitions = partition_by -%}
  {%- endif -%}

IFF(
  LEAD({{ valid_from }}) OVER (
    PARTITION BY {{ partitions }}
    ORDER   BY {{ valid_from }}
  ) IS NULL,
  TRUE,
  FALSE
) AS valid_version
{%- endmacro %}
