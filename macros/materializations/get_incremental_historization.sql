{#
Macro Name: get_incremental_historization_sql

Description:
    Incremental strategy that can be used for historization.
    We use the INSERT_ONLY strategy, i.e. if any changes arrive, the new entries get inserted but the old entries will never be deleted or updated.
    Moreover, if an old entry does not exist in the new data anymore, it will be inserted again with the CO_IS_DELETED flag.
    More precisely, we have 4 different cases:
    
    1. A new entry is not in the DWH table yet:
        Insert the new entry with (CO_VALID_FROM, DSS_LOADTIMESTAMP, CO_IS_DELETED) = (current_date, current_timestamp, false).
    
    2. An entry exists in the new data and in the DWH table. The new entry differs from the old one:
        The old entry will not be chagned.
        Insert the new entry with (CO_VALID_FROM, DSS_LOADTIMESTAMP, CO_IS_DELETED) = (current_date, current_timestamp, false).
    
    3. An entry exists in the new data and in the DWH table. The new entry and the old entry are are equal:
        Nothing happens. The new entry will not be inserted into the DWH table.
    
    4. An old entry does not occur in the new data anymore:
        Insert the old entry again with (CO_VALID_FROM, DSS_LOADTIMESTAMP, CO_IS_DELETED) = (current_date, current_timestamp, true).

Remarks:
    - The DSS_LOADTIMESTAMP is necessary, if an entry gets changed more than once a day, e.g. if wrong data has to be corrected.

    - It is recommended to use this macro with the config parameter tmp_relation_type='view' because its default is 'table'.

    - Your model must contain the lines:

        select ...
            current_date as CO_VALID_FROM,
            current_timestamp as DSS_LOADTIMESTAMP,
            false as CO_IS_DELETED
        from ...

      This is necessary to fill these columns in case of creating a new table or in case of a full refresh.

    - By default, the macro uses all columns for comparison except the technical DSS_ columns. You can change this behavior with the compare_columns parameter.

    - The default column names CO_VALID_FROM, DSS_LOADTIMESTAMP and CO_IS_DELETED can be changed, see below.

Arguments:
    unique_key (string or list)         : Required. List of columns of primary key.
    compare_columns (string or list)    : Optional. List of columns used to compare new and old entries.
    valid_from_column (string)          : Optional. Alternative name of CO_VALID_FROM column.
    load_timestamp_column (string)      : Optional. Alternative name of DSS_LOAD_TIMESTAMP column.
    is_deleted_column (string)          : Optional. Alternative name of CO_IS_DELETED column.

Returns:
    SQL statement

Example:
    {{ config(
        materialized='incremental',
        incremental_strategy='historization',
        tmp_relation_type='view',
        unique_key = 'sap_order_id',
        compare_columns = ['sap_kst', 'sap_bukrs']
    ) }}
#}

{% macro get_incremental_historization_sql(arg_dict) %}

    {# Read config parameters. #}
    {%- set compare_columns         = config.meta_get('compare_columns', []) -%}
    {%- set valid_from_column       = config.meta_get('valid_from_column', 'CO_VALID_FROM').upper() -%}
    {%- set load_timestamp_column   = config.meta_get('load_timestamp_column', 'DSS_LOAD_TIMESTAMP').upper() -%}
    {%- set is_deleted_column       = config.meta_get('is_deleted_column', 'CO_IS_DELETED').upper() -%}

    {# Read from arg_dict. #}
    {%- set dest_cols   = arg_dict["dest_columns"] | map(attribute="name") | list -%}
    {%- set dwh_table   = arg_dict["target_relation"] -%}
    {%- set new_data    = arg_dict["temp_relation"] -%}
    {%- set unique_key  = arg_dict["unique_key"] -%}


    {# check if data is available in source table to avoid all rows would be set to deleted. #}
    {%- if execute -%}
        {%- set check_sql -%}
            select count(*) from {{ new_data }}
        {%- endset -%}
        
        {%- set result = run_query(check_sql) -%}
        {%- set row_count = result.columns[0].values()[0] | int -%}
        
        {%- if row_count == 0 -%}
            {{ log("INFO: No new data found in " ~ new_data ~ ". Skipping historization.", info=True) }}
            {# We return a neutral SQL statement that triggers no action #}
            {% do return("select 'Macro terminated: No source data found.'") %}
        {%- endif -%}
    {%- endif -%}

    {# Define unique key. #}
    {%- if unique_key is none -%}
        {%- do exceptions.raise_compiler_error('A unique key is required.') -%}
    {%- endif -%}

    {%- if unique_key is string -%}
        {%- set unique_key = [unique_key] -%}
    {%- endif -%}

    {%- set unique_key_csv = unique_key | join(', ') -%}


    {# Define destination columns. #}
    {%- set dest_cols_clean     = dest_cols | reject('eq', valid_from_column) | reject('eq', load_timestamp_column) | reject('eq', is_deleted_column) | list -%}

    {%- set dest_cols_clean_csv = dest_cols_clean | join(', ') -%}


    {# Define compare columns. #}
    {%- if compare_columns is string -%}
        {%- set compare_columns = [compare_columns] -%}
    {%- endif -%}

    {%- if compare_columns == [] -%}
        {%- for col in dest_cols_clean -%}
            {%- if not col.startswith('DSS_') -%}
                {%- do compare_columns.append(col) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- else -%}
        {%- set compare_columns = compare_columns | map('upper') | list -%}
    {%- endif -%}

    {%- if valid_from_column in compare_columns or load_timestamp_column in compare_columns or is_deleted_column in compare_columns -%}
        {%- do exceptions.raise_compiler_error('The "compare_columns" parameter must not contain the columns for "valid_from", "load_timestamp" or "is_deleted".') -%}
    {%- endif -%}


    {# Define final insert statement. #}
    {%- set dml -%}
        insert into {{dwh_table}} (
            {{dest_cols_clean_csv}}, {{valid_from_column}}, {{load_timestamp_column}}, {{is_deleted_column}}
        )
        with cs as (
            select * from (
                select * from {{dwh_table}}
                qualify row_number() over (partition by {{unique_key_csv}} order by {{valid_from_column}} desc nulls last, {{load_timestamp_column}} desc nulls last) = 1
            )
            where {{is_deleted_column}} = false
        ),
        diff as (
            select
                {% for key in unique_key %}{{'coalesce(new.' ~ key ~ ', old.' ~ key ~ ') as ' ~ key ~ ','}}{% endfor %}
                dbt_new_diff_key,
                dbt_old_diff_key
            from (
                select
                    {{unique_key_csv}},
                    {{generate_sha2_hash(compare_columns)}} as dbt_new_diff_key
                from {{new_data}}
            ) new
            full outer join (
                select
                    {{unique_key_csv}},
                    {{generate_sha2_hash(compare_columns)}} as dbt_old_diff_key
                from cs
            ) old
            on {% for key in unique_key %}
            new.{{key}} = old.{{key}}{% if not loop.last %} and {% endif %}
            {%- endfor %}
        )

        select
            {% for col in dest_cols_clean %}{{'new.' ~ col ~ ','}}{% endfor %}
            current_date as {{valid_from_column}},
            current_timestamp as {{load_timestamp_column}},
            false as {{is_deleted_column}}
        from {{new_data}} new
        inner join diff
        on {% for key in unique_key %}new.{{key}} = diff.{{key}} and {% endfor %}
            (diff.dbt_old_diff_key is null or diff.dbt_old_diff_key != diff.dbt_new_diff_key)

        union all

        select
        {% for col in dest_cols_clean %}{{'cs.' ~ col ~ ','}}{% endfor %}
        current_date as {{valid_from_column}},
        current_timestamp as {{load_timestamp_column}},
        true as {{is_deleted_column}}
        from cs
        inner join diff
        on {% for key in unique_key %}cs.{{key}} = diff.{{key}} and {% endfor %}
            diff.dbt_new_diff_key is null
    {%- endset -%}

    {% do return(snowflake_dml_explicit_transaction(dml)) %}

{% endmacro %}