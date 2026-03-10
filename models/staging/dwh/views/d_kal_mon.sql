-- -------------------------------------------------------------------------
-- Model Name   : d_kal_mon
-- File         : models/staging/dwh/views/d_kal_mon.sql
-- Layer        : staging
-- Pipeline     : # TODO: add pipeline name if not provided
-- Description  : source model for D_KAL_MON
-- Author       : auto-generated
-- Created      : 2025-11-10
-- -------------------------------------------------------------------------

--------------------------------------------------------------
-- Model Configuration
--------------------------------------------------------------
{{ config(
    materialized='view',
    alias='src_d_kal_mon'
) }}

--------------------------------------------------------------
-- SQL Statement
--------------------------------------------------------------
select
    kal_mon_id,
    qurt_id,
    kal_mon_txt
from {{ source('dwh_dwh', 'd_kal_mon') }}
