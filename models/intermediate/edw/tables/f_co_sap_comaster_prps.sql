-- -------------------------------------------------------------------------
-- Model Name   : f_co_sap_comaster_prps
-- File         : models/intermediate/edw/tables/f_co_sap_comaster_prps.sql
-- Layer        : edw
-- Pipeline     : p60load
-- Description  : Loads and typecasts PRPS data from RAW to EDW (valid rows).
-- Author       : auto-generated
-- Created      : 2025-08-22
-- -------------------------------------------------------------------------

{% set warehouse = {
    'DEV': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'TEST': 'PRODUCT_CO_DEV_CUSTOM_X1',
    'INT': 'PRODUCT_CO_INT_CUSTOM_TM1_TEC_X1',
    'PROD': 'PRODUCT_CO_PROD_CUSTOM_TM1_TEC_X1'
} %}

{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns',
    snowflake_warehouse=warehouse[env_var('DBT_ENV_TYPE')],
    tags='p60load',
    pre_hook=[
      "
      DELETE FROM {{ ref('raw_co_sap_comaster_prps') }}
      WHERE load_timestamp IS NOT NULL
        AND load_timestamp NOT IN (
          SELECT load_timestamp
          FROM (
            SELECT load_timestamp,
                   DENSE_RANK() OVER (ORDER BY load_timestamp DESC) AS r
            FROM {{ ref('raw_co_sap_comaster_prps') }}
            WHERE load_timestamp IS NOT NULL
          )
          WHERE r <= 5
        )
      "
    ]
  )
}}

with src as (
  select * from {{ ref('raw_co_sap_comaster_prps') }}
),

{% if is_incremental() %}
existing_keys as ( select distinct dss_file_content_key from {{ this }} ),
max_loaded  as ( select coalesce(max(stag_timestamp), date('1950-01-01')) as max_ts from {{ this }} ),
{% else %}
existing_keys as ( select cast(null as varchar) as dss_file_content_key where 1=0 ),
max_loaded  as ( select date('1950-01-01') as max_ts ),
{% endif %}

typed as (
  select
      dss_file_name,
      dss_file_content_key,
      dss_file_start_scan_time,
      dss_row_number,
      load_timestamp                                              as stag_timestamp,
      try_to_number(replace(stufe, ',', ''), 3)                   as stufe,
      left(posid, 24)                                             as posid,
      left(pspnr, 8)                                              as pspnr,
      left(post1, 40)                                             as post1,
      left(objnr, 22)                                             as objnr,
      try_to_number(replace(psphi, ',', ''), 8)                   as psphi,
      left(poski, 16)                                             as poski,
      left(ernam, 12)                                             as ernam,
      try_to_date(erdat, 'YYYY.MM.DD')                            as erdat,
      left(aenam, 12)                                             as aenam,
      try_to_date(aedat, 'YYYY.MM.DD')                            as aedat,
      left(prart, 2)                                              as prart,
      left(plakz, 1)                                              as plakz,
      left(belkz, 1)                                              as belkz,
      left(fakkz, 1)                                              as fakkz,
      left(pkokr, 4)                                              as pkokr,
      left(pbukr, 4)                                              as pbukr,
      left(prctr, 10)                                             as prctr,
      left(pwpos, 3)                                              as pwpos,
      left(slwid, 7)                                              as slwid,
      left(scope, 2)                                              as scope,
      left(verna, 25)                                             as verna,
      left(imprf, 6)                                              as imprf,
      try_to_number(replace(vernr, ',', ''), 8)                   as vernr,
      try_to_number(replace(astnr, ',', ''), 8)                   as astnr,
      left(astna, 25)                                             as astna,
      left(pgsbr, 4)                                              as pgsbr,
      left(npfaz, 1)                                              as npfaz,
      try_to_number(replace(zuord, ',', ''), 1)                   as zuord,
      left(trmeq, 1)                                              as trmeq,
      left(kvewe, 1)                                              as kvewe,
      left(kappl, 2)                                              as kappl,
      left(kalsm, 6)                                              as kalsm,
      left(zschl, 6)                                              as zschl,
      left(abgsl, 6)                                              as abgsl,
      left(akokr, 4)                                              as akokr,
      left(akstl, 10)                                             as akstl,
      left(fkokr, 4)                                              as fkokr,
      left(fkstl, 10)                                             as fkstl,
      left(fabkl, 2)                                              as fabkl,
      left(pspri, 1)                                              as pspri,
      left(equnr, 18)                                             as equnr,
      left(tplnr, 30)                                             as tplnr,
      left(werks, 4)                                              as werks,
      left(txtsp, 1)                                              as txtsp,
      left(usr00, 20)                                             as usr00,
      left(usr01, 20)                                             as usr01,
      left(usr02, 10)                                             as usr02,
      left(usr03, 10)                                             as usr03,
      try_to_number(replace(usr04, ',', ''), 13)                  as usr04,
      left(use04, 3)                                              as use04,
      try_to_number(replace(usr05, ',', ''), 13)                  as usr05,
      left(use05, 3)                                              as use05,
      try_to_number(replace(usr06, ',', ''), 13)                  as usr06,
      left(use06, 5)                                              as use06,
      try_to_number(replace(usr07, ',', ''), 13)                  as usr07,
      left(use07, 5)                                              as use07,
      try_to_date(usr08, 'YYYY.MM.DD')                            as usr08,
      try_to_date(usr09, 'YYYY.MM.DD')                            as usr09,
      left(usr10, 1)                                              as usr10,
      left(usr11, 1)                                              as usr11,
      left(kostl, 10)                                             as kostl,
      left(ktrg, 12)                                              as ktrg,
      left(berst, 16)                                             as berst,
      left(bertr, 16)                                             as bertr,
      left(berko, 16)                                             as berko,
      left(berbu, 16)                                             as berbu,
      left(clasf, 1)                                              as clasf,
      try_to_number(replace(spsnr, ',', ''), 8)                   as spsnr,
      left(xstat, 1)                                              as xstat,
      left(txjcd, 15)                                             as txjcd,
      left(zschm, 7)                                              as zschm,
      try_to_number(replace(evgew, ',', ''), 8)                   as evgew,
      left(aennr, 12)                                             as aennr,
      left(subpr, 12)                                             as subpr,
      left(postu, 40)                                             as postu,
      left(plint, 1)                                              as plint,
      left(loevm, 1)                                              as loevm,
      left(kzbws, 1)                                              as kzbws,
      left(fplnr, 10)                                             as fplnr,
      try_to_date(tadat, 'YYYY.MM.DD')                            as tadat,
      left(izwek, 2)                                              as izwek,
      left(isize, 2)                                              as isize,
      left(iumkz, 5)                                              as iumkz,
      left(abukr, 4)                                              as abukr,
      left(grpkz, 1)                                              as grpkz,
      left(pgprf, 6)                                              as pgprf,
      left(logsystem, 10)                                         as logsystem,
      try_to_number(replace(pspnr_logs, ',', ''), 8)              as pspnr_logs,
      left(stort, 10)                                             as stort,
      left(func_area, 16)                                         as func_area,
      left(klvar, 4)                                              as klvar,
      try_to_number(replace(kalnr, ',', ''), 12)                  as kalnr,
      left(posid_edit, 24)                                        as posid_edit,
      left(pspkz, 1)                                              as pspkz,
      left(matnr, 40)                                             as matnr,
      try_to_number(replace(vlpsp, ',', ''), 8)                   as vlpsp,
      left(vlpkz, 1)                                              as vlpkz,
      left(sort1, 10)                                             as sort1,
      left(sort2, 10)                                             as sort2,
      left(sort3, 10)                                             as sort3,
      left(vname, 6)                                              as vname,
      left(recid, 2)                                              as recid,
      left(etype, 3)                                              as etype,
      left(otype, 4)                                              as otype,
      left(jibcl, 3)                                              as jibcl,
      left(jibsa, 5)                                              as jibsa,
      left(cgpl_guid16, 32)                                       as cgpl_guid16,
      left(cgpl_logsys, 10)                                       as cgpl_logsys,
      left(cgpl_objtype, 3)                                       as cgpl_objtype,
      left(adpsp, 40)                                             as adpsp,
      left(rfippnt, 20)                                           as rfippnt,
      left(eew_prps_ps_dummy, 1)                                  as eew_prps_ps_dummy,
      left(rfund, 10)                                             as rfund,
      left(rgrant_nbr, 20)                                        as rgrant_nbr,
      left(fund_fix_assign, 1)                                    as fund_fix_assign,
      left(grant_fix_assigned, 1)                                 as grant_fix_assigned,
      left(func_area_fix_assigned, 1)                             as func_area_fix_assigned,
      left(sponsoredprog, 20)                                     as sponsoredprog,
      try_to_number(replace(cpd_updat, ',', ''), 15)              as cpd_updat,
      left(ferc_ind, 4)                                           as ferc_ind,
      try_to_number(replace(posnr_prps, ',', ''), 6)              as posnr_prps,
      left(vbeln_prps, 10)                                        as vbeln_prps
  from src
  where
    load_timestamp > (select max_ts from max_loaded)
    and dss_file_content_key not in (select dss_file_content_key from existing_keys)
    and (stufe is null or try_to_number(replace(stufe, ',', ''),3) is not null)
    and (posid is null or left(posid, 24) = posid)
    and (pspnr is null or left(pspnr, 8) = pspnr)
    and (post1 is null or left(post1, 40) = post1)
    and (objnr is null or left(objnr, 22) = objnr)
    and (psphi is null or try_to_number(replace(psphi, ',', ''),8) is not null)
    and (poski is null or left(poski, 16) = poski)
    and (ernam is null or left(ernam, 12) = ernam)
    and (erdat is null or try_to_date(erdat, 'YYYY.MM.DD') is not null)
    and (aenam is null or left(aenam, 12) = aenam)
    and (aedat is null or try_to_date(aedat, 'YYYY.MM.DD') is not null)
    and (prart is null or left(prart, 2) = prart)
    and (plakz is null or left(plakz, 1) = plakz)
    and (belkz is null or left(belkz, 1) = belkz)
    and (fakkz is null or left(fakkz, 1) = fakkz)
    and (pkokr is null or left(pkokr, 4) = pkokr)
    and (pbukr is null or left(pbukr, 4) = pbukr)
    and (prctr is null or left(prctr, 10) = prctr)
    and (pwpos is null or left(pwpos, 3) = pwpos)
    and (slwid is null or left(slwid, 7) = slwid)
    and (scope is null or left(scope, 2) = scope)
    and (verna is null or left(verna, 25) = verna)
    and (imprf is null or left(imprf, 6) = imprf)
    and (vernr is null or try_to_number(replace(vernr, ',', ''),8) is not null)
    and (astnr is null or try_to_number(replace(astnr, ',', ''),8) is not null)
    and (astna is null or left(astna, 25) = astna)
    and (pgsbr is null or left(pgsbr, 4) = pgsbr)
    and (npfaz is null or left(npfaz, 1) = npfaz)
    and (zuord is null or try_to_number(replace(zuord, ',', ''),1) is not null)
    and (trmeq is null or left(trmeq, 1) = trmeq)
    and (kvewe is null or left(kvewe, 1) = kvewe)
    and (kappl is null or left(kappl, 2) = kappl)
    and (kalsm is null or left(kalsm, 6) = kalsm)
    and (zschl is null or left(zschl, 6) = zschl)
    and (abgsl is null or left(abgsl, 6) = abgsl)
    and (akokr is null or left(akokr, 4) = akokr)
    and (akstl is null or left(akstl, 10) = akstl)
    and (fkokr is null or left(fkokr, 4) = fkokr)
    and (fkstl is null or left(fkstl, 10) = fkstl)
    and (fabkl is null or left(fabkl, 2) = fabkl)
    and (pspri is null or left(pspri, 1) = pspri)
    and (equnr is null or left(equnr, 18) = equnr)
    and (tplnr is null or left(tplnr, 30) = tplnr)
    and (werks is null or left(werks, 4) = werks)
    and (txtsp is null or left(txtsp, 1) = txtsp)
    and (usr00 is null or left(usr00, 20) = usr00)
    and (usr01 is null or left(usr01, 20) = usr01)
    and (usr02 is null or left(usr02, 10) = usr02)
    and (usr03 is null or left(usr03, 10) = usr03)
    and (usr04 is null or try_to_number(replace(usr04, ',', ''),13) is not null)
    and (use04 is null or left(use04, 3) = use04)
    and (usr05 is null or try_to_number(replace(usr05, ',', ''),13) is not null)
    and (use05 is null or left(use05, 3) = use05)
    and (usr06 is null or try_to_number(replace(usr06, ',', ''),13) is not null)
    and (use06 is null or left(use06, 5) = use06)
    and (usr07 is null or try_to_number(replace(usr07, ',', ''),13) is not null)
    and (use07 is null or left(use07, 5) = use07)
    and (usr08 is null or try_to_date(usr08, 'YYYY.MM.DD') is not null)
    and (usr09 is null or try_to_date(usr09, 'YYYY.MM.DD') is not null)
    and (usr10 is null or left(usr10, 1) = usr10)
    and (usr11 is null or left(usr11, 1) = usr11)
    and (kostl is null or left(kostl, 10) = kostl)
    and (ktrg  is null or left(ktrg, 12)  = ktrg)
    and (berst is null or left(berst, 16) = berst)
    and (bertr is null or left(bertr, 16) = bertr)
    and (berko is null or left(berko, 16) = berko)
    and (berbu is null or left(berbu, 16) = berbu)
    and (clasf is null or left(clasf, 1) = clasf)
    and (spsnr is null or try_to_number(replace(spsnr, ',', ''),8) is not null)
    and (xstat is null or left(xstat, 1) = xstat)
    and (txjcd is null or left(txjcd, 15) = txjcd)
    and (zschm is null or left(zschm, 7) = zschm)
    and (evgew is null or try_to_number(replace(evgew, ',', ''),8) is not null)
    and (aennr is null or left(aennr, 12) = aennr)
    and (subpr is null or left(subpr, 12) = subpr)
    and (postu is null or left(postu, 40) = postu)
    and (plint is null or left(plint, 1) = plint)
    and (loevm is null or left(loevm, 1) = loevm)
    and (kzbws is null or left(kzbws, 1) = kzbws)
    and (fplnr is null or left(fplnr, 10) = fplnr)
    and (tadat is null or try_to_date(tadat, 'YYYY.MM.DD') is not null)
    and (izwek is null or left(izwek, 2) = izwek)
    and (isize is null or left(isize, 2) = isize)
    and (iumkz is null or left(iumkz, 5) = iumkz)
    and (abukr is null or left(abukr, 4) = abukr)
    and (grpkz is null or left(grpkz, 1) = grpkz)
    and (pgprf is null or left(pgprf, 6) = pgprf)
    and (logsystem is null or left(logsystem, 10) = logsystem)
    and (pspnr_logs is null or try_to_number(replace(pspnr_logs, ',', ''),8) is not null)
    and (stort is null or left(stort, 10) = stort)
    and (func_area is null or left(func_area, 16) = func_area)
    and (klvar is null or left(klvar, 4) = klvar)
    and (kalnr is null or try_to_number(replace(kalnr, ',', ''),12) is not null)
    and (posid_edit is null or left(posid_edit, 24) = posid_edit)
    and (pspkz is null or left(pspkz, 1) = pspkz)
    and (matnr is null or left(matnr, 40) = matnr)
    and (vlpsp is null or try_to_number(replace(vlpsp, ',', ''),8) is not null)
    and (vlpkz is null or left(vlpkz, 1) = vlpkz)
    and (sort1 is null or left(sort1, 10) = sort1)
    and (sort2 is null or left(sort2, 10) = sort2)
    and (sort3 is null or left(sort3, 10) = sort3)
    and (vname is null or left(vname, 6) = vname)
    and (recid is null or left(recid, 2) = recid)
    and (etype is null or left(etype, 3) = etype)
    and (otype is null or left(otype, 4) = otype)
    and (jibcl is null or left(jibcl, 3) = jibcl)
    and (jibsa is null or left(jibsa, 5) = jibsa)
    and (cgpl_guid16 is null or left(cgpl_guid16, 32) = cgpl_guid16)
    and (cgpl_logsys is null or left(cgpl_logsys, 10) = cgpl_logsys)
    and (cgpl_objtype is null or left(cgpl_objtype, 3) = cgpl_objtype)
    and (adpsp is null or left(adpsp, 40) = adpsp)
    and (rfippnt is null or left(rfippnt, 20) = rfippnt)
    and (eew_prps_ps_dummy is null or left(eew_prps_ps_dummy, 1) = eew_prps_ps_dummy)
    and (rfund is null or left(rfund, 10) = rfund)
    and (rgrant_nbr is null or left(rgrant_nbr, 20) = rgrant_nbr)
    and (fund_fix_assign is null or left(fund_fix_assign, 1) = fund_fix_assign)
    and (grant_fix_assigned is null or left(grant_fix_assigned, 1) = grant_fix_assigned)
    and (func_area_fix_assigned is null or left(func_area_fix_assigned, 1) = func_area_fix_assigned)
    and (sponsoredprog is null or left(sponsoredprog, 20) = sponsoredprog)
    and (cpd_updat is null or try_to_number(replace(cpd_updat, ',', ''),15) is not null)
    and (ferc_ind is null or left(ferc_ind, 4) = ferc_ind)
    and (posnr_prps is null or try_to_number(replace(posnr_prps, ',', ''),6) is not null)
    and (vbeln_prps is null or left(vbeln_prps, 10) = vbeln_prps)
)

select * from typed
