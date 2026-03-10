-- -------------------------------------------------------------------------
-- Macro Name   : detect_duplicates_across_edw
-- File         : macros/tests/detect_duplicates_across_edw.sql
-- Purpose      : Detects duplicate records across staging tables and existing EDW tables
-- Description  : This macro implements sophisticated duplicate detection logic across
--                three tiers: EDW existing records, same-file duplicates, and 
--                cross-file duplicates. It uses configurable business keys and 
--                content hashes to identify exact duplicates with proper precedence
--                handling. Gracefully handles cases where EDW tables don't exist yet.
-- Author       : dbt_product_co team
-- Created      : 2025
-- -------------------------------------------------------------------------
--
-- FUNCTIONALITY:
-- - Three-tier duplicate detection with proper precedence:
--   1. EDW duplicates (highest priority - always flagged)
--   2. Same-file duplicates (intra-file duplicate detection)
--   3. Cross-file duplicates (inter-file duplicate detection)
-- - Handles non-existent EDW tables gracefully (first-run scenarios)
-- - Uses business keys and content hashes for exact duplicate identification
-- - Configurable column mapping for different table structures
-- - Returns standardized error records compatible with audit logging
-- - Implements timestamp and filename-based precedence rules
--
-- DUPLICATE DETECTION LOGIC:
-- 1. DUPLICATE_IN_EDW: Record exists in target EDW table (always duplicate)
-- 2. DUPLICATE_IN_SAME_FILE: Multiple records with same key/hash in same file
--    - Later timestamps take precedence
--    - Lower row numbers take precedence for same timestamp
-- 3. DUPLICATE_IN_OTHER_FILE: Same key/hash across different files
--    - Later timestamps take precedence
--    - Lexicographically later filenames take precedence for same timestamp
--
-- PARAMETERS:
-- @stg_table              (required): Reference to staging table being validated
-- @edw_table              (required): Target EDW table name for comparison
-- @business_key_column    (optional): Business key column name (default: 'dss_hash_key')
-- @hash_column            (optional): Content hash column name (default: 'dss_hash_diff')
-- @last_modified_column   (optional): Timestamp column name (default: 'dss_last_modified')
-- @file_name_column       (optional): File name column name (default: 'dss_file_name')
-- @row_number_column      (optional): Row number column name (default: 'dss_row_number')
-- @source_identifier_column (optional): Column name containing business identifier 
--   (e.g., invoice number) for linking errors to raw data (default: 'dss_hash_key')
--
-- RETURNED COLUMNS:
-- - dss_file_name: Source file name
-- - dss_row_number: Row number from source file
-- - dss_last_modified: File last modified timestamp
-- - dss_hash_key: Business key that was duplicated
-- - source_data_identifier: Business identifier (configurable via source_identifier_column)
-- - rule_name: Always 'no_duplicate_allowed'
-- - field_name: Name of the business key column
-- - invalid_value: The duplicate business key value
--
-- USAGE EXAMPLES:
--
-- 1. Standard Usage (from f_co_sap_paid_fi_items_error.sql):
--    co_sap_paid_fi_items_duplicate as (
--        {{ detect_duplicates_across_edw(
--            stg_table=ref('tf_co_sap_paid_fi_items'),
--            edw_table=table_ref,
--            source_identifier_column='wbeln'
--        ) }}
--    )
--
-- 2. Custom Column Mapping:
--    {{ detect_duplicates_across_edw(
--        stg_table=ref('my_staging_table'),
--        edw_table='edw.my_target_table',
--        business_key_column='my_business_key',
--        hash_column='my_content_hash',
--        last_modified_column='my_timestamp',
--        file_name_column='my_file_name',
--        source_identifier_column='invoice_number'
--    ) }}
--
-- 3. Using Default Hash Key as Identifier:
--    {{ detect_duplicates_across_edw(
--        stg_table=ref('new_staging_table'),
--        edw_table='edw.not_yet_created_table'
--    ) }}
--    -- Uses dss_hash_key as source_data_identifier by default
--
-- INTEGRATION PATTERN:
-- Typically used in error tables alongside data quality rules:
--   with data_quality_errors as (
--       {{ assert_data_quality_rules(...) }}
--   ),
--   duplicate_errors as (
--       {{ detect_duplicates_across_edw(...) }}
--   )
--   select * from data_quality_errors
--   union all
--   select * from duplicate_errors
--
-- REQUIREMENTS:
-- - Staging table must have standard metadata columns
-- - EDW table (if exists) must have matching column structure
-- - Business key and content hash columns must exist in both tables
-- - Proper indexing recommended on business key columns for performance
--
-- PRECEDENCE RULES:
-- - EDW records always take precedence over staging records
-- - Later timestamps take precedence over earlier ones
-- - For same timestamp: lower row numbers (same file) or later filenames (cross-file)
-- - Same file duplicates take precedence over cross-file duplicates
--
-- -------------------------------------------------------------------------

{% macro detect_duplicates_across_edw(
    stg_table,
    edw_table,
    business_key_column='dss_hash_key',
    hash_column='dss_hash_diff',
    last_modified_column='dss_last_modified',
    file_name_column='dss_file_name',
    row_number_column='dss_row_number',
    source_identifier_column='dss_hash_key'
) %}

    -- Load source data
    with source_data as (
        select
            {{ business_key_column }} as dss_hash_key,
            {{ hash_column }} as dss_hash_diff,
            dss_load_date,
            {{ file_name_column }} as dss_file_name,
            {{ row_number_column }} as dss_row_number,
            {{ source_identifier_column }} as source_data_identifier,
            {{ last_modified_column }} as dss_last_modified
        from {{ stg_table }}
    ),

    {% set edw_table_exists = adapter.get_relation(
        database=edw_table.split('.')[0] if '.' in edw_table else target.database,
        schema=edw_table.split('.')[1] if '.' in edw_table and edw_table.split('.')|length == 3 else (edw_table.split('.')[0] if '.' in edw_table else target.schema),
        identifier=edw_table.split('.')[-1]
    ) %}

    {% if edw_table_exists %}
    -- Load matching edw data
    edw_data as (
        select 
            edw.{{ business_key_column }} as dss_hash_key,
            edw.{{ hash_column }} as dss_hash_diff,
            null as dss_load_date, -- Mark as EDW data
            edw.{{ file_name_column }} as dss_file_name,
            edw.{{ row_number_column }} as dss_row_number,
            edw.{{ source_identifier_column }} as source_data_identifier,
            edw.{{ last_modified_column }} as dss_last_modified
        from {{ edw_table }} as edw
        inner join source_data as src
            on src.dss_hash_key = edw.{{ business_key_column }}
    ),
    {% else %}
    -- EDW table doesn't exist yet (first run) - use empty dataset
    edw_data as (
        select 
            cast(null as varchar) as dss_hash_key,
            cast(null as varchar) as dss_hash_diff,
            cast(null as timestamp) as dss_load_date,
            cast(null as varchar) as dss_file_name,
            cast(null as number) as dss_row_number,
            cast(null as varchar) as source_data_identifier,
            cast(null as timestamp) as dss_last_modified
        from {{ stg_table }}
        where 1 = 0
    ),
    {% endif %}

    unioned_data as (
        select * from source_data
        union all
        select * from edw_data
    ),

    ranked_data as (
        select 
            *,
            -- Rank by exact duplicates (same business key AND same content)
            row_number() over (
                partition by dss_hash_key, dss_hash_diff
                order by dss_last_modified desc, dss_file_name desc
            ) as exact_duplicate_rank,
            -- Rank by business key only (for version tracking)
            row_number() over (
                partition by dss_hash_key
                order by dss_last_modified desc, dss_file_name desc
            ) as version_rank,
            -- Mark source origin
            case 
                when dss_load_date is null then 'EDW'
                else 'SOURCE'
            end as data_origin
        from unioned_data
    ),
    
    -- Identify true duplicates: records from source that have exact matches in EDW or earlier in source
    duplicate_analysis as (
        select 
            src.*,
            -- Check if this source record has an exact duplicate with higher precedence
            case 
                -- First: Check for exact duplicates in EDW (always flag as duplicate)
                when exists (
                    select 1 
                    from ranked_data edw 
                    where edw.dss_hash_key = src.dss_hash_key 
                    and edw.dss_hash_diff = src.dss_hash_diff
                    and edw.data_origin = 'EDW'
                ) then 'DUPLICATE_IN_EDW'
                
                -- Second: Check for duplicates within the same file (intra-file duplicates)
                when exists (
                    select 1 
                    from ranked_data same_file_dup 
                    where same_file_dup.dss_hash_key = src.dss_hash_key 
                    and same_file_dup.dss_hash_diff = src.dss_hash_diff
                    and same_file_dup.data_origin = 'SOURCE'
                    and same_file_dup.dss_file_name = src.dss_file_name
                    and (same_file_dup.dss_last_modified > src.dss_last_modified 
                         or (same_file_dup.dss_last_modified = src.dss_last_modified 
                             and same_file_dup.dss_row_number < src.dss_row_number))
                ) then 'DUPLICATE_IN_SAME_FILE'
                
                -- Third: Check for duplicates in different source files (inter-file duplicates)
                when exists (
                    select 1 
                    from ranked_data diff_file_dup 
                    where diff_file_dup.dss_hash_key = src.dss_hash_key 
                    and diff_file_dup.dss_hash_diff = src.dss_hash_diff
                    and diff_file_dup.data_origin = 'SOURCE'
                    and diff_file_dup.dss_file_name != src.dss_file_name
                    and (diff_file_dup.dss_last_modified > src.dss_last_modified 
                         or (diff_file_dup.dss_last_modified = src.dss_last_modified 
                             and diff_file_dup.dss_file_name > src.dss_file_name))
                ) then 'DUPLICATE_IN_OTHER_FILE'
                
                else 'VALID'
            end as duplicate_status
        from source_data src
    )

    select 
        dss_file_name,
        dss_row_number,
        dss_last_modified,
        dss_hash_key,
        source_data_identifier,
        'no_duplicate_allowed' as rule_name,
        '{{ business_key_column }}' as field_name,
        dss_hash_key as invalid_value
    from duplicate_analysis
    where duplicate_status in ('DUPLICATE_IN_EDW', 'DUPLICATE_IN_SAME_FILE', 'DUPLICATE_IN_OTHER_FILE')

{% endmacro %}
