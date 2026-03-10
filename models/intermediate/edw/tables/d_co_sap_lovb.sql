-- -------------------------------------------------------------------------
-- Model Name   : d_co_sap_lovb
-- File         : models/intermediate/edw/tables/d_co_sap_lovb.sql
-- Layer        : intermediate
-- Pipeline     : conbwstamm
-- Description  : SAP BW Stammdaten zum Logistikvertriebsbereich - SAP BW masterdata for logistics sales area
-- Author       : auto-generated
-- Created      : 2025-09-24
-- -------------------------------------------------------------------------

--------------------------------------------------------------
-- Warehouse Settings
--------------------------------------------------------------
{% set warehouse = {
    'DEV': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'TEST': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'INT': 'PRODUCT_CO_INT_CUSTOM_ETL_TEC_X1',
    'PROD': 'PRODUCT_CO_PROD_CUSTOM_ETL_TEC_X1'
} %}

--------------------------------------------------------------
-- Model Configuration
--------------------------------------------------------------
{{ config(
    materialized='incremental',
    incremental_strategy = 'historization',
    tmp_relation_type='view',
    unique_key='sap_lovb_id',
    snowflake_warehouse=warehouse[env_var('DBT_ENV_TYPE')],
    tags=['conbwstamm']
) }}


--------------------------------------------------------------
-- SQL Statement
--------------------------------------------------------------

-- get master data
with lu_d_ma_bereich as (
    select
        ma_ber_nr,
        ma_ber_txt
    from {{ ref('sum_lu_d_ma_bereich') }}
    where ma_ber_nr != 00
),

-- get manuel input
zcd_kst_lovb as (
    select
        zcm_kst_lovb,
        zcm_kst_lovb_kurztxt
    from {{ source('co_write', 'zcd_kst_lovb') }}
),

-- get max_id to create new ids for new ma_ber_nr
max_id as (
    select max(sap_lovb_id) as max_id
    from {{ this }}
),

-- get ids for new and existing ma_ber_nr
get_ids as (
    -- create new id for new ma_ber_nr
    select distinct
        row_number() over (order by dmb.ma_ber_nr) + max_id.max_id as sap_lovb_id,
        dmb.ma_ber_nr as sap_lovb_nr
    from lu_d_ma_bereich as dmb
    left join max_id
        on 1 = 1
    where dmb.ma_ber_nr not in (select distinct sap_lovb_nr from {{ this }}) -- noqa

    union distinct

    -- get existing ids for existing ma_ber_nr
    select distinct
        sap_lovb_id,
        sap_lovb_nr
    from {{ this }}
),

-- get current values to compare for changes
current_values as (
    select * from {{ this }}
    where co_is_deleted = 0
    qualify row_number() over (partition by sap_lovb_id order by dwh_guelt_von desc) = 1
),

-- create full load for historization --> if no change in record, the old record is kept, missing ones are set to inactive and new ones are added
final as (
    select
        ids.sap_lovb_id,
        dmb.ma_ber_nr as sap_lovb_nr,
        coalesce(dmb.ma_ber_txt, curr.sap_lovb_txt, '') as sap_lovb_txt,
        coalesce(zkl.zcm_kst_lovb_kurztxt, curr.sap_lovb_kurztxt, '') as sap_lovb_kurztxt
    from lu_d_ma_bereich as dmb
    left join get_ids as ids
        on dmb.ma_ber_nr = ids.sap_lovb_nr
    left join zcd_kst_lovb as zkl
        on dmb.ma_ber_nr = zkl.zcm_kst_lovb
    left join current_values as curr
        on ids.sap_lovb_id = curr.sap_lovb_id

    union distinct

    -- Dummy record
    select
        1000 as sap_lovb_id,
        '' as sap_lovb_nr,
        'Keine Zuordnung' as sap_lovb_txt,
        'Keine Zuordnung' as sap_lovb_kurztxt
)

select
    sap_lovb_id,
    sap_lovb_nr,
    sap_lovb_txt,
    sap_lovb_kurztxt
from final
