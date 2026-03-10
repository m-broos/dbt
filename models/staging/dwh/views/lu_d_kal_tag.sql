-- -------------------------------------------------------------------------
-- Model Name   : lu_d_kal_tag
-- File         : models/staging/dwh/views/lu_d_kal_tag.sql
-- Layer        : staging
-- Pipeline     : # TODO: add pipeline name if not provided
-- Description  : Lookup Tabelle für Kalendertage (Business Calendar)
-- Author       : auto-generated
-- Created      : 2025-10-21
-- -------------------------------------------------------------------------

--------------------------------------------------------------
-- Model Configuration
--------------------------------------------------------------
{{ config(
    materialized='view',
    alias='src_lu_d_kal_tag'
) }}

--------------------------------------------------------------
-- SQL Statement
--------------------------------------------------------------
select
    kal_tag_id,
    wtag_id,
    erfa_wo_id,
    kal_tag_txt,
    kal_mon_id,
    mon_nr,
    kal_wo_id,
    qurt_id,
    jhr_id,
    log_jhr,
    log_qurt,
    log_mon,
    log_wo,
    kal_wo_otw_id,
    log_wo_otw,
    vj_kal_wo_tag_id,
    vj_kal_wo_tag_txt,
    h_jhr_id,
    log_h_jhr,
    kal_teilwoche_id
from {{ source('dwh_dwh', 'lu_d_kal_tag') }}
