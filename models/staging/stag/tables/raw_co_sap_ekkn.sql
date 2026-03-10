-- -------------------------------------------------------------------------
-- Model Name   : raw_co_sap_ekkn
-- File         : models/staging/stag/tables/raw_co_sap_ekkn.sql
-- Layer        : staging
-- Pipeline     : ekkn
-- Description  : Fact table for EKKN from parquet file in GCP bucket
-- Author       : auto-generated
-- Created      : 2025-12-02
-- -------------------------------------------------------------------------


--------------------------------------------------------------
-- Warehouse Settings
--------------------------------------------------------------

{% set env = env_var('DBT_ENV_TYPE', 'DEV') %}

{%- set warehouse={
    'DEV': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'TEST': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'INT': 'PRODUCT_CO_INT_CUSTOM_ETL_TEC_X1',
    'PROD': 'PRODUCT_CO_PROD_CUSTOM_ETL_TEC_X1'}
-%}


--------------------------------------------------------------
-- Model Configuration
--------------------------------------------------------------

{{
    config(
        materialized = 'copy_into',
        snowflake_warehouse = warehouse[env_var('DBT_ENV_TYPE', 'DEV')],
        post_hook = [
        "{{ audit_create_table_if_not_exists('h_extract_data_load_audit_log') }}",
        "{{ audit_logging_insert_statement(
            pipeline_name='ekkn',
            audit_table='dma.h_extract_data_load_audit_log',
            source_table='external_stages.gcs_stage_ekkn',
            created_by='dbt_ekkn_pipeline'
        )}}"
        ],
        tags = ['ekkn']
    )
}}

--------------------------------------------------------------
-- SQL Statement
--------------------------------------------------------------

select
    metadata$filename as dss_file_name,
    metadata$file_content_key as dss_file_content_key,
    metadata$file_last_modified as dss_last_modified,
    metadata$start_scan_time as dss_file_start_scan_time,
    metadata$file_row_number as dss_row_number,
    current_timestamp() as dss_load_timestamp,
    '{{ var("ctm_run_id") }}'::varchar(10) as dss_ctm_run_id,
    regexp_substr(metadata$filename, '_(P60_([0-9_]+))_part', 1, 1, 'e', 2) as dss_file_timestamp,
    $1:EBELN::varchar as ebeln,
    $1:EBELP::varchar as ebelp,
    $1:GRANT_NBR::varchar as grant_nbr,
    $1:KOKRS::varchar as kokrs,
    $1:KOSTL::varchar as kostl,
    $1:PRCTR::varchar as prctr,
    $1:PS_PSP_PNR::varchar as ps_psp_pnr,
    $1:SAKTO::varchar as sakto,
    $1:WEMPF::varchar as wempf
from
    @{{ source('external_stages_sources', 'gcs_stage_theobald') }}
    (
        file_format => 'external_stages.parquet__no_header__comma_separator',
        pattern => 'Purchase_Order/EKKN/.*EKKN.*[.]parquet'
    )
