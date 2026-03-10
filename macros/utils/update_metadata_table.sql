{#
Macro Name: update_metadata_table

Description:
    Updates a metadata table with information about latest pipeline run.
    Must be used in an incremental - model with incremental strategy 'merge', see example below.

    As a required parameter, the macro takes the name of the staging table containing the new data.
    Optionally, you can add names for following tables:
        - A table containing valid entries only.
        - A table containing error entries only.
        - A table that contains former errors which have been corrected.
    
    Optionally, the parameter file_dicard marks every fails with an 'X' if it contains at least one error.

Arguments:
    staging_table (string)          : Name of the staging table.
    table_valid_entries (string)    : Table that contains all valid entries (default: none).
    table_errors (string)           : Table that contains all errors (default: none).
    table_corrected_errors (string) : Table that contains all corrected errors (default: none).
    file_dicard (boolean)           : If true, every file that contains at least one error record is marked as discarded (default: false).

Returns:
    SQL statement: Select statement to be merged into metadata table.

Example:
    {{ config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='dss_file_name',
        tags='tm1nonfivp'
    ) }}

    {{ update_metadata_table(
        staging_table=ref('tf_co_tm1_nonfi_vp'),
        table_valid_entries=ref('f_co_tm1_nonfi_vp'),
        table_errors=ref('f_co_tm1_nonfi_vp_error')
    ) }}
#}

{% macro update_metadata_table(
    staging_table,
    table_valid_entries=none,
    table_errors=none,
    table_corrected_errors=none,
    file_dicard=false) %}
    {%- set statement -%}
    select
         stg.dss_file_name
        ,stg.dss_load_date
        ,stg.lfd_nr_load
        ,stg.lfd_nr_rohdat
        ,current_timestamp as load_metadata_timestamp
        ,stg.amount::number(38, 0) as total_entries
        {% if table_valid_entries %},coalesce(valid.amount, 0)::number(38, 0) as valid_entries{% endif %}
        {% if table_errors %},coalesce(errors.amount, 0)::number(38, 0) as error_entries{% endif %}
        {% if table_corrected_errors %},coalesce(corrected_errors.amount, 0)::number(38, 0) as corrected_errors_entries{% endif %}
        {% if table_errors and file_dicard %},case when errors.amount > 0 then 'X' else '' end as file_discard{% endif %} 
    from (
        select
        dss_file_name,
        dss_load_date,
        lfd_nr_load,
        lfd_nr_rohdat,
        count(*) amount
        from {{staging_table}}
        group by 1, 2, 3, 4
    ) stg
    {% if table_valid_entries %}
    left join (
        select
            lfd_nr_rohdat,
            count(*) amount
            from {{table_valid_entries}}
            group by lfd_nr_rohdat
    ) valid
    on stg.lfd_nr_rohdat = valid.lfd_nr_rohdat
    {% endif %}
    {% if table_errors %}
    left join (
        select
            lfd_nr_rohdat,
            count(*) amount
            from {{table_errors}}
            group by lfd_nr_rohdat
    ) errors
    on stg.lfd_nr_rohdat = errors.lfd_nr_rohdat
    {% endif %}
    {% if table_corrected_errors %}
    left join (
        select
            lfd_nr_rohdat,
            count(*) amount
            from {{table_corrected_errors}}
            group by lfd_nr_rohdat
    ) corrected_errors
    on stg.lfd_nr_rohdat = corrected_errors.lfd_nr_rohdat
    {% endif %}
    {%- endset -%}

    {{- statement -}}
{% endmacro %}