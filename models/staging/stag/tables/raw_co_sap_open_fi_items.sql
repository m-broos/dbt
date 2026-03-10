-- -------------------------------------------------------------------------
-- Model Name   : raw_co_sap_open_fi_items
-- File         : models/staging/stag/tables/raw_co_sap_open_fi_items.sql
-- Layer        : staging
-- Pipeline     : wcmofizz
-- Description  : Raw load of SAP open FI items from external stage (WCMOFIZZ)
-- Author       : auto-generated
-- Created      : 2025-06-05
-- -------------------------------------------------------------------------

--------------------------------------------------------------
-- Warehouse Settings
--------------------------------------------------------------
{% set warehouse = {
    'DEV': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'TEST': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'INT': 'PRODUCT_CO_INT_CUSTOM_TEC_X1',
    'PROD': 'PRODUCT_CO_PROD_CUSTOM_TEC_X1'
} %}

--------------------------------------------------------------
-- Model Configuration
--------------------------------------------------------------
{{ config(
    materialized = 'copy_into',
    transient = false,
    snowflake_warehouse=warehouse[env_var('DBT_ENV_TYPE')],
    post_hook=[
        "{{ audit_create_table_if_not_exists('h_wcm_data_load_audit_log') }}",
        "{{ audit_logging_insert_statement(
            pipeline_name='wcmofizz',
            audit_table='dma.h_wcm_data_load_audit_log',
            source_table='external_stages.gcs_stage_sf_wcmofizz',
            created_by='dbt_wcm_pipeline'
        )}}"
    ],
    tags = ['wcmofizz']
) }}

--------------------------------------------------------------
-- SQL Statement
--------------------------------------------------------------
select
    -- Metadata columns
    current_date as dss_load_date, -- Load date of the data.
    metadata$file_row_number as dss_row_number,  -- Row number from the source file.
    metadata$filename as dss_file_name,          -- Name of the source file.
    metadata$file_content_key as dss_file_content_key,  -- Unique key for the file content.
    metadata$file_last_modified as dss_last_modified,  -- Last modified timestamp of the file.
    '{{ var("ctm_run_id") }}'::varchar(10) as dss_ctm_run_id,

    -- Data columns
    $1 as bukrs,               -- Company code.
    $2 as gjahr,               -- Fiscal year.
    $3 as belnr,               -- Document number.
    $4 as lifnr,               -- Vendor account number.
    $5 as empfb,               -- Receiver.
    $6 as warenlieferant,      -- Goods supplier.
    $7 as warenlieferant_gln,  -- Goods supplier GLN (Global Location Number).
    $8 as zzaq_eiww,           -- Custom field (description needed).
    $9 as kunre,               -- Customer account number.
    $10 as budat,              -- Posting date.
    $11 as bldat,              -- Document date.
    $12 as xblnr,              -- Reference document number.
    $13 as kidno,              -- Customer order number.
    $14 as blart,              -- Document type.
    $15 as zterm,              -- Payment terms.
    $16 as zfbdt,              -- Baseline date for payment.
    $17 as zbd1t,              -- Payment block date.
    $18 as zlspr,              -- Payment method.
    $19 as netwr,              -- Net amount.
    $20 as mwsbp,              -- Tax amount.
    $21 as brtwr,              -- Gross amount.
    $22 as zahlbetrag,         -- Payment amount.
    $23 as zahldatum,          -- Payment date.
    $24 as zwels,              -- Payment method supplement.
    $25 as blart_rzf,          -- Document type for clearing.
    $26 as ekkol,              -- Purchasing document.
    $27 as tax_rate,           -- Tax rate.
    $28 as utp_kz,             -- Custom field (description needed).
    $29 as basedate_tsa        -- Base date for TSA.

from
    @{{ source('external_stages_sources', 'gcs_stage_sf_wcmofizz') }}
    (
        file_format => 'EXTERNAL_STAGES.CSV__WITH_HEADER__SEMICOLON_SEPARATOR',
        pattern => '.*DAILY_OPEN_FI_ITEMS.*[.]csv'
    )
