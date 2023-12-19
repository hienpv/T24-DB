--------------------------------------------------------
--  DDL for Procedure PR_SYNC_MDB_PRODUCT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PR_SYNC_MDB_PRODUCT" 
IS
BEGIN

    --#20150701 LOctx add for: tam thoi loai bo MDB theo san pham, gio khong dung nua
    DELETE FROM IBS.bk_acct_product_type_MDB;

    INSERT INTO IBS.bk_acct_product_type_MDB
      (product_type,
       ccy_code,
       description,
       acc_type)
      SELECT TRIM(sccode),
              TRIM(dp2cur),
              TRIM(pscdes),
              'DD'
       FROM   RAWSTAGE.SI_PAR_DDPAR2@RAWSTAGE_PRO_CORE
       WHERE PSLDES like '%MDB%' or PSCDES like '%MDB%'
       ;

    INSERT INTO IBS.bk_acct_product_type_MDB
        (
          product_type,
          description,
          short_name,
          group_code,
          group_description,
          acc_type,
          ccy_code
        )
     SELECT
         TRIM(ptype),
         TRIM(pdesc),
         TRIM(psdsc),
         pgroup,
         TRIM(pgrpds),
         'CD',
         TRIM(pcurty)
     FROM
         RAWSTAGE.SI_PAR_CDPAR2@RAWSTAGE_PRO_CORE
         WHERE PLDESC like '%MDB%' or pdesc like '%MDB%'
    ;

    INSERT INTO IBS.bk_acct_product_type_MDB
      (product_type,
       description,
       group_code,
       group_description,
       ccy_code,
       acc_type)
      SELECT TRIM(ptype),
              TRIM(ptydsc),
              plngrp,
              TRIM(pgrdsc),
              TRIM(pcurty),
              'LN'
       FROM   RAWSTAGE.SI_PAR_LNPAR2@RAWSTAGE_PRO_CORE
       WHERE ptydsc LIKE '%MDB%' or pgrdsc  LIKE '%MDB%'

    ;
END;

/
