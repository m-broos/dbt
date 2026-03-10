-- -------------------------------------------------------------------------
-- Model Name   : d_kal_tag
-- File         : models/staging/dwh/views/d_kal_tag.sql
-- Layer        : staging
-- Pipeline     : # TODO: add pipeline name if not provided
-- Description  : source model for D_KAL_TAG
-- Author       : auto-generated
-- Created      : 2025-11-10
-- -------------------------------------------------------------------------

--------------------------------------------------------------
-- Model Configuration
--------------------------------------------------------------
{{ config(
    materialized='view',
    alias='src_d_kal_tag'
) }}

--------------------------------------------------------------
-- SQL Statement
--------------------------------------------------------------
select
    kal_tag_id,
    wtag_id,
    erfa_wo_id,
    kal_tag_txt
from {{ source('dwh_dwh', 'd_kal_tag') }}
