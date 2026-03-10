{% macro generate_sha2_hash(column_list) -%}
SHA2(
  concat(
    {%- for col in column_list -%}
      IFNULL(
        NULLIF(
          TRIM(CAST({{ col }} AS VARCHAR)),
          ''
        ),
        '^$^'
      )
      {%- if not loop.last -%}, '||$$$||',{%- endif -%}
    {%- endfor -%}
  ),
  256
)
{%- endmacro %}
