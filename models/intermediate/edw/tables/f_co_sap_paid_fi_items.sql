-- -------------------------------------------------------------------------
-- Model Name   : f_co_sap_paid_fi_items
-- File         : models/intermediate/edw/tables/f_co_sap_paid_fi_items.sql
-- Layer        : intermediate
-- Pipeline     : wcmpaidfizzday
-- Description  : Fact table for SAP paid FI items (imported from source table)
-- Author       : auto-generated
-- Created      : 2025-07-08
-- -------------------------------------------------------------------------

--------------------------------------------------------------
-- Warehouse Settings
--------------------------------------------------------------
{% set warehouse = {
    'DEV': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'TEST': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'INT': 'PRODUCT_CO_INT_CUSTOM_WCM_TEC_X2',
    'PROD': 'PRODUCT_CO_PROD_CUSTOM_WCM_TEC_X2'
} %}

--------------------------------------------------------------
-- Model Configuration
--------------------------------------------------------------
{{ config(
    materialized='incremental',
    incremental_strategy = 'append',
    cluster_by=['dss_file_name','dss_last_modified'],
    snowflake_warehouse=warehouse[env_var('DBT_ENV_TYPE')],
    post_hook=[
        "{{ audit_create_table_if_not_exists('h_wcm_data_load_audit_log') }}",
        "{{ audit_logging_insert_statement(
            pipeline_name='wcmpaidfizzday',
            audit_table='dma.h_wcm_data_load_audit_log',
            source_table='stag.raw_co_sap_paid_fi_items',
            error_table='edw.f_co_sap_paid_fi_items_error',
            created_by='dbt_wcm_pipeline'
        )}}"
    ],
    tags=['wcmpaidfizzday']
) }}

--------------------------------------------------------------
-- SQL Statement
--------------------------------------------------------------

with error_data as (
    select distinct
        dss_file_name,
        dss_hash_key,
        dss_last_modified,
        (case when rule_name = 'no_duplicate_allowed' then 'duplicate' else 'error' end) as dss_row_status
    from {{ ref('f_co_sap_paid_fi_items_error') }}
)

select
    src.sap_bukrs_id,
    src.sap_kreditoren_id,
    src.lief_id,
    src.bukrs,
    src.wbeln,
    src.lifre,
    src.zzax_ilnnr,
    src.zzaq_eiww,
    src.wfdat,
    src.bldat,
    src.xblnr,
    src.kidno,
    src.netwr,
    src.mwsbk,
    src.brtwr,
    src.zz_faedt,
    src.augdt,
    src.ekkol,
    src.zbd1t,
    src.zz_zbd1td,
    src.skonto_prz,
    src.zahltag,
    src.zwels,
    src.uzawe,
    src.zz_vat_rate,
    src.zzab_utp_kz,
    src.zzab_basisdt_tsa,
    src.dss_hash_key,
    src.dss_hash_diff,
    src.dss_row_number,
    src.dss_file_name,
    src.dss_load_date,
    src.dss_last_modified,
    src.dss_ctm_run_id,
    err.dss_row_status
from {{ ref('tf_co_sap_paid_fi_items') }} as src
left join error_data as err
    on src.dss_file_name = err.dss_file_name
    and src.dss_hash_key = err.dss_hash_key
    and src.dss_last_modified = err.dss_last_modified
where err.dss_row_status != 'duplicate' -- noqa
    or err.dss_row_status is null
