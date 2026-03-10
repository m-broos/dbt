{#
    This macro generates a calendar month ID (kal_mon_id) from separate month and year columns.
    - If the month is greater than 12, it sets the month to 'M12'.
    - If the year is 0, it sets the month to 'M01'.
    - Otherwise, it combines the year and month in the format 'YYYY/Mmm' (e.g., 2023/M11).
    - The output column name can be customized (default: kal_mon_id).
    Useful for standardizing period keys in ETL/ELT pipelines.

    Example usage (from a dbt model select):

    {{ create_kal_mon_id('stg.tm1_monat', 'stg.tm1_jahr') }}
    # Note: 'stg' is the table alias for the staging/source table in the SQL query.
#}

{% macro create_kal_mon_id(month_column, year_column, output_column='kal_mon_id') %}
    {%- set statement -%}
    case
        when try_to_number({{month_column}}) > 12 then {{year_column}} || '/M12'
        when try_to_number({{year_column}}) = 0 then {{year_column}} || '/M01'
        else {{year_column}} || '/M' || lpad({{month_column}}, 2, '0')
    end as {{output_column}}
    {%- endset -%}
    {{- statement -}}
{% endmacro %}