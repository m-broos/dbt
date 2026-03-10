-- -------------------------------------------------------------------------
-- Model Name   : stag_z_glue_t001
-- File         : models/staging/stag//views/stag_z_glue_t001.sql
-- Layer        : stag
-- Pipeline     : stag_z_glue_t001
-- Description  : View for snp replication for z_glue_t001
-- Author       : Data Engineer
-- Created      : 2026-02-19
-- -------------------------------------------------------------------------

-- --------------------------------------------------------------
-- Model Configuration
-- --------------------------------------------------------------
{{ config(
    materialized='view'
) }}

-- --------------------------------------------------------------
-- SQL Statement
-- --------------------------------------------------------------

select
    mandt,
    bukrs,
    glrequest,
    butxt,
    ort01,
    land1,
    waers,
    spras,
    ktopl,
    waabw,
    periv,
    kokfi,
    rcomp,
    adrnr,
    stceg,
    fikrs,
    xfmco,
    xfmcb,
    xfmca,
    txjcd,
    fmhrdate,
    xtemplt,
    transit_plant,
    buvar,
    fdbuk,
    xfdis,
    xvalv,
    xskfn,
    kkber,
    xmwsn,
    mregl,
    xgsbe,
    xgjrv,
    xkdft,
    xprod,
    xeink,
    xjvaa,
    xvvwa,
    xslta,
    xfdmm,
    xfdsd,
    xextb,
    ebukr,
    ktop2,
    umkrs,
    bukrs_glob,
    fstva,
    opvar,
    xcovr,
    txkrs,
    wfvar,
    xbbbf,
    xbbbe,
    xbbba,
    xbbko,
    xstdt,
    mwskv,
    mwska,
    impda,
    xnegp,
    xkkbi,
    wt_newwt,
    pp_pdate,
    infmt,
    fstvare,
    kopim,
    dkweg,
    offsacct,
    bapovar,
    xcos,
    xcession,
    xsplt,
    surccm,
    dtprov,
    dtamtc,
    dttaxc,
    dttdsp,
    dtaxr,
    xvatdate,
    pst_per_var,
    xbbsc,
    f_obsolete,
    fm_derive_acc,
    gldelflag,
    glchangetime
from {{ source('snp_landing_zone', 'z_glue_t001') }}
