-- -------------------------------------------------------------------------
-- Macro Name   : audit_logging_insert_statement
-- File         : macros/logging/audit_logging_insert_statement.sql
-- Purpose      : Inserts comprehensive audit log entries for dbt pipeline executions
-- Description  : This macro creates detailed audit trail records capturing pipeline
--                execution metadata, data quality metrics, file processing statistics,
--                and dbt runtime information. It prevents duplicate audit entries
--                and supports both external stage and table-based data sources.
-- Author       : dbt_product_co team
-- Created      : 2025
-- -------------------------------------------------------------------------
--
-- FUNCTIONALITY:
-- - Creates audit log entries with comprehensive metadata capture
-- - Prevents duplicate audit entries within 1-minute execution windows
-- - Supports external stages and regular table sources
-- - Captures data quality metrics (valid, error, duplicate row counts)
-- - Records dbt runtime metadata (run ID, duration, materialization type)
-- - Generates incremental load numbers for file tracking
-- - Handles error table integration for data quality reporting
--
-- PARAMETERS:
-- @pipeline_name    (required): Name of the data pipeline being audited
-- @audit_table      (required): Fully qualified audit table name (schema.table)
-- @source_table     (required): Source table/stage name (schema.table or external_stages.stage_name)
-- @error_table      (optional): Error table for data quality metrics (schema.table)
-- @created_by       (optional): Creator identifier (default: 'dbt')
-- @capture_metadata (optional): Enable dbt runtime metadata capture (default: true)
--
-- USAGE EXAMPLES:
--
-- 1. Basic External Stage Audit (from raw_co_sap_paid_fi_items.sql):
--    post_hook=[
--        "{{ audit_create_table_if_not_exists('h_wcm_data_load_audit_log') }}",
--        "{{ audit_logging_insert_statement(
--            pipeline_name='wcmpaidfizzday',
--            audit_table='dma.h_wcm_data_load_audit_log',
--            source_table='external_stages.gcs_stage_sf_wcmpaidfizzday',
--            created_by='dbt_wcm_pipeline'
--        )}}"
--    ]
--
-- 2. Advanced Usage with Error Tracking:
--    post_hook=[
--        "{{ audit_logging_insert_statement(
--            pipeline_name='my_pipeline',
--            audit_table='audit.pipeline_logs',
--            source_table='staging.source_data',
--            error_table='error.data_quality_errors',
--            created_by='dbt_custom_pipeline',
--            capture_metadata=true
--        )}}"
--    ]
--
-- 3. Table-to-Table Migration Audit:
--    post_hook=[
--        "{{ audit_logging_insert_statement(
--            pipeline_name='legacy_migration',
--            audit_table='audit.migration_logs',
--            source_table='legacy.old_table',
--            created_by='migration_process'
--        )}}"
--    ]
--
-- AUDIT TABLE SCHEMA REQUIREMENTS:
-- The audit table must exist with columns matching audit_create_table_if_not_exists macro:
-- - pipeline_name, dss_load_date, dss_ctm_run_id, dss_file_name
-- - source_table, target_table, target_schema, target_load_number
-- - valid_row_count, error_row_count, duplicate_row_count, load_status
-- - created_by, dbt_run_id, dbt_model_name, dbt_run_started_at, etc.
--
-- DEPENDENCIES:
-- - get_static_table macro for table reference resolution
-- - audit_create_table_if_not_exists macro (should be called before this macro)
-- - Target table must have metadata columns: dss_file_name, dss_last_modified, dss_ctm_run_id
--
-- -------------------------------------------------------------------------

{% macro audit_logging_insert_statement(
    pipeline_name,
    audit_table,
    source_table,
    error_table=none,
    created_by='dbt',
    capture_metadata=true
) %}

    {#-- Check if audit_table is fully qualified --#}
    {%- if '.' in audit_table -%}
        {%- set parts = audit_table.split('.') -%}
        {%- if parts | length == 2 -%}
            {%- set audit_table_schema = parts[0] -%}
            {%- set audit_table_name = parts[1] -%}
            
            {#-- Resolve table references --#}
            {% set audit_table_resolved = get_static_table(audit_table_name, audit_table_schema) %}

        {%- else -%}
            {{ log("Debug: Invalid parts length: " ~ (parts | length), info=True) }}
            {{ exceptions.raise_compiler_error("Invalid audit table format. Use 'schema.table'. Got " ~ (parts | length) ~ " parts: " ~ parts) }}
        {%- endif -%}
    {%- else -%}
        {{ log("Debug: No dot found in audit_table", info=True) }}
        {{ exceptions.raise_compiler_error("Invalid audit table format. Use 'schema.table'. No dot found in: '" ~ audit_table ~ "'") }}
    {%- endif -%}

    {#-- Check if source_table is fully qualified --#}
    {%- if 'external_stages.' not in  source_table -%}
        {%- if '.' in source_table -%}
            {%- set parts = source_table.split('.') -%}
            {%- if parts | length == 2 -%}
                {%- set source_table_schema = parts[0] -%}
                {%- set source_table_name = parts[1] -%}

                {#-- Resolve table references --#}
                {% set source_table_resolved = get_static_table(source_table_name, source_table_schema) %}

            {%- else -%}
                {{ exceptions.raise_compiler_error("Invalid source table format. Use 'schema.table'") }}
            {%- endif -%}
        {%- else -%}
            {{ exceptions.raise_compiler_error("Invalid source table format. Use 'schema.table'") }}
        {%- endif -%}
    {%- endif -%}

    {#-- Check if error_table is fully qualified --#}
    {%- if error_table is not none -%}
        {%- if '.' in error_table -%}
            {%- set parts = error_table.split('.') -%}
            {%- if parts | length == 2 -%}
                {%- set error_table_schema = parts[0] -%}
                {%- set error_table_name = parts[1] -%}
                
                {#-- Resolve table references --#}
                {% set error_table_resolved = get_static_table(error_table_name, error_table_schema) %}

            {%- else -%}
                {{ exceptions.raise_compiler_error("Invalid error table format. Use 'schema.table'") }}
            {%- endif -%}
        {%- else -%}
            {{ exceptions.raise_compiler_error("Invalid error table format. Use 'schema.table'") }}
        {%- endif -%}
    {% endif %}

    {% set target_table_resolved = this.database ~ '.' ~ this.schema ~ '.' ~ this.identifier %}
    {% set target_schema = this.schema %}

    {#-- Insert audit log entry --#}
    {#-- First check if an entry for this execution already exists --#}
    {% set check_existing_query %}
        select count(*) as existing_count
        from {{ audit_table_resolved }}
        where pipeline_name = '{{ pipeline_name }}'
        and target_table = '{{ target_table_resolved }}'
        and dss_load_date >= current_timestamp - interval '1 minute'
        and created_by = '{{ created_by }}'
    {% endset %}

    {% set existing_results = run_query(check_existing_query) %}
    {% if existing_results and existing_results.columns[0].values()[0] == 0 %}
    
    insert into {{ audit_table_resolved }} (
        pipeline_name,
        dss_load_date,
        dss_ctm_run_id,
        dss_file_name,
        dss_last_modified,
        source_table,
        target_table,
        target_schema,
        target_load_number,
        valid_row_count,
        error_row_count,
        duplicate_row_count,
        load_status,
        created_by,
        {% if capture_metadata %}
            dbt_run_id,
            dbt_model_name,
            dbt_run_started_at,
            dbt_run_duration_seconds,
            dbt_model_unique_id,
            dbt_version,
            dbt_target,
            dbt_materialization
        {% endif %}
    )

    {% if error_table is not none %}
        with error_logging as (
            select
                dss_file_name,
                dss_last_modified,
                count(distinct (case when rule_name != 'no_duplicate_allowed' then dss_row_number else null end)) as error_row_count,
                count(distinct (case when rule_name = 'no_duplicate_allowed' then invalid_value else null end)) as duplicate_row_count
            from {{ error_table_resolved }}
            group by 
                dss_file_name, 
                dss_last_modified
        ),
    {% else %}
        with error_logging as (
            select
                null as dss_file_name,
                null as dss_last_modified,
                0 as error_row_count,
                0 as duplicate_row_count
        ),
    {% endif %}
    
    valid_records as (
        select 
            dss_file_name,
            dss_last_modified,
            count(*) as valid_row_count
        from {{ target_table_resolved }}
        group by 
            dss_file_name, 
            dss_last_modified
    )

    select
        '{{ pipeline_name }}' as pipeline_name,
        current_timestamp as dss_load_date,
        max(src.dss_ctm_run_id) as dss_ctm_run_id,
        src.dss_file_name,
        src.dss_last_modified,
        {% if 'external_stages.' in source_table %}
            '{{ source_table }}' as source_table,
        {% else %}
            '{{ source_table_resolved }}' as source_table,
        {% endif %}
        '{{ target_table_resolved }}' as target_table,
        '{{ target_schema }}' as target_schema,
        -- Generate incremental load number for each file in this execution
        (coalesce(
            (select max(target_load_number) 
             from {{ audit_table_resolved }} 
             where pipeline_name = '{{ pipeline_name }}' 
             and target_table = '{{ target_table_resolved }}'), 0) + 
         dense_rank() over (order by src.dss_file_name)
        ) as target_load_number,
        max(coalesce(val.valid_row_count, 0)) as valid_row_count,
        {% if error_table is not none %}
            max(coalesce(err.error_row_count, 0)) as error_row_count,
            max(coalesce(err.duplicate_row_count, 0)) as duplicate_row_count,
            (case when max(coalesce(val.valid_row_count, 0)) = 0 or 
                (max(coalesce(val.valid_row_count, 0)) = max(coalesce(err.error_row_count, 0))) or
                (max(coalesce(val.valid_row_count, 0)) = max(coalesce(err.duplicate_row_count, 0)))
            then 'skipped' else
            (case when max(coalesce(val.valid_row_count, 0)) > 0 and 
                    (max(coalesce(err.error_row_count, 0)) > 0 or
                     max(coalesce(err.duplicate_row_count, 0)) > 0
                    ) then 'completed with error' else
                'completed' end)end) as load_status,
        {% else %}
            0 as error_row_count,
            0 as duplicate_row_count,
            (case when max(coalesce(val.valid_row_count, 0)) = 0 then 'skipped' else
            'completed' end) as load_status,
        {% endif %}
        '{{ created_by }}' as created_by{% if capture_metadata %},
        -- dbt Run Metadata
        '{{ invocation_id }}' as dbt_run_id,
        '{{ this.identifier }}' as dbt_model_name,
        '{{ run_started_at.strftime('%Y-%m-%d %H:%M:%S') }}' as dbt_run_started_at,
        datediff('second', '{{ run_started_at.strftime('%Y-%m-%d %H:%M:%S') }}'::timestamp, current_timestamp) as dbt_run_duration_seconds,
        '{{ this.database }}.{{ this.schema }}.{{ this.identifier }}' as dbt_model_unique_id,
        '{{ dbt_version }}' as dbt_version,
        '{{ target.name }}' as dbt_target,
        '{{ config.get("materialized", "unknown") }}' as dbt_materialization
        {% endif %}
    {% if 'external_stages.' in source_table %}
        from {{ target_table_resolved }} as src
    {% else %}
        from {{ source_table_resolved }} as src
    {% endif %}
    left join error_logging as err
        on src.dss_file_name = err.dss_file_name
        and src.dss_last_modified = err.dss_last_modified
    left join valid_records as val
        on src.dss_file_name = val.dss_file_name
        and src.dss_last_modified = val.dss_last_modified
    group by
        src.dss_file_name,
        src.dss_last_modified
    
    {% else %}
        {#-- Entry already exists for this execution, skip insert --#}
        select 'Audit entry already exists for this execution - skipping duplicate insert' as message
    {% endif %}
{% endmacro %}
