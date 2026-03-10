-- -------------------------------------------------------------------------
-- Macro Name   : audit_create_table_if_not_exists
-- File         : macros/logging/audit_create_table_if_not_exists.sql
-- Purpose      : Creates audit logging table for dbt pipeline monitoring
-- Author       : Data Engineering Team
-- Created      : 2025-07-24
-- -------------------------------------------------------------------------

/**
 * Creates a centralized audit table for tracking dbt model executions and data quality metrics.
 * 
 * Description:
 *   This macro creates a standardized audit table that captures comprehensive metadata
 *   about dbt model runs, including file processing statistics, error counts, load status,
 *   and dbt runtime information. The table is clustered for optimal query performance.
 *
 * Purpose:
 *   - Track data pipeline execution history and performance
 *   - Monitor data quality metrics (valid/error/duplicate row counts)
 *   - Provide audit trail for compliance and troubleshooting
 *   - Enable pipeline observability and monitoring dashboards
 *
 * Table Schema:
 *   Core Audit Fields:
 *     - pipeline_name: Name of the data pipeline
 *     - load_status: Status of the load (completed, error, skipped)
 *     - target_load_number: Incremental load sequence number
 *   
 *   Data Quality Metrics:
 *     - valid_row_count: Number of successfully processed rows
 *     - error_row_count: Number of rows with data quality issues
 *     - duplicate_row_count: Number of duplicate rows detected
 *   
 *   File Processing Metadata:
 *     - dss_file_name: Source file name
 *     - dss_file_last_modified: File modification timestamp
 *   
 *   dbt Runtime Metadata:
 *     - dbt_run_id: Unique identifier for dbt execution
 *     - dbt_model_name: Name of the dbt model
 *     - dbt_run_duration_seconds: Model execution time
 *     - dbt_materialization: Type of materialization used
 *     - dbt_version: dbt version used for execution
 *
 *   Data migration data:
 *     - mig_lfd_rohdat: Migration field for raw data
 *
 * Parameters:
 *   @param audit_table (string): Name of the audit table to create (without schema)
 *
 * Usage Examples:
 *   
 *   Basic usage in post_hook:
 *   {{ config(
 *       post_hook=[
 *           "{{ audit_create_table_if_not_exists('h_wcm_data_load_audit_log') }}"
 *       ]
 *   ) }}
 *   
 *   Combined with audit logging:
 *   {{ config(
 *       post_hook=[
 *           "{{ audit_create_table_if_not_exists('h_wcm_data_load_audit_log') }}",
 *           "{{ audit_logging_insert_statement(
 *               pipeline_name='wcmpaidfizzday',
 *               audit_table='dma.h_wcm_data_load_audit_log',
 *               source_table='edw_dwh.f_sap_zahlungsziel',
 *               created_by='dbt_wcm_pipeline',
 *               capture_metadata=true
 *           )}}"
 *       ]
 *   ) }}
 *
 * Notes:
 *   - Table is created with clustering on (pipeline_name, dss_load_date) for performance
 *   - Only creates table if it doesn't already exist (idempotent operation)
 *   - Uses 'dma' schema by default via get_static_table() function
 *   - Skips creation during incremental runs to avoid unnecessary DDL operations
 *
 * Dependencies:
 *   - get_static_table() macro for table name resolution
 *   - is_incremental() function for conditional execution
 */

{% macro audit_create_table_if_not_exists(audit_table) %}
    {% if not is_incremental() %}
        create table if not exists {{ get_static_table(audit_table, 'dma') }} cluster by (pipeline_name, dss_load_date) (
            pipeline_name             varchar,
            dss_load_date             timestamp,
            dss_ctm_run_id            varchar,
            dss_file_name             varchar,
            dss_last_modified         timestamp,
            source_table              varchar,
            target_table              varchar,
            target_schema             varchar,
            target_load_number        number,
            valid_row_count           number,
            error_row_count           number,
            duplicate_row_count       number,
            load_status               varchar,
            created_by                varchar,
            dbt_run_id                 varchar,
            dbt_model_name              varchar,
            dbt_run_started_at          timestamp,
            dbt_run_duration_seconds    number,
            dbt_model_unique_id         varchar,
            dbt_materialization         varchar,
            dbt_version                varchar,
            dbt_target                 varchar,
            mig_lfd_nr_rohdat           number
        );
    {% endif %}
{% endmacro %}
