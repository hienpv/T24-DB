--------------------------------------------------------
--  DDL for Package Body CSPKG_PARAM_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."CSPKG_PARAM_SYNC" 
IS
/*----------------------------------------------------------------------------------------------------
     ** Module   : COMMODITY SYSTEM
     ** and is copyrighted by FSS.
     **
     **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
     **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
     **    graphic, optic recording or otherwise, translated in any language or computer language,
     **    without the prior written permission of Financial Software Solutions. JSC.
     **
     **  MODIFICATION HISTORY
     **  Person      Date           Comments
     **  HaoNS      09-SEP-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/


 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **  Call khi co CFTNEW, Luu y: bang sync_cfoffl duoc dong bo 1-1 tu CDC,
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_fee_discount_sync(dm_operation_type in CHAR,
                                    orchestrate_cfcifn in NUMBER,
                                    orchestrate_cfsnme in VARCHAR,
                                    orchestrate_cfbust in VARCHAR,
                                    orchestrate_cfoffr in VARCHAR)
    IS
        l_err_desc VARCHAR2(250);
    BEGIN
          CSPKS_CDC_T24_UTIL.PR_T24_CDD_LOG_CFTNEW ( dm_operation_type  , orchestrate_cfcifn  ,  orchestrate_cfsnme  ,
                                      orchestrate_cfbust  , orchestrate_cfoffr  , 0 , 0 , 'CDD INPUT' ) ;
        IF dm_operation_type <> 'D' THEN
            MERGE INTO bk_fee_discount c
            USING (SELECT DISTINCT orchestrate_cfcifn AS cfcifn,
                                   TRIM(orchestrate_cfsnme) AS cfsnme,
                                   TRIM(orchestrate_cfbust) AS cfbust,
                                   TRIM(b.cfoffr) AS cfoffr
                   FROM (SELECT orchestrate_cfcifn ,
                                    orchestrate_cfsnme,
                                    orchestrate_cfbust ,
                                    orchestrate_cfoffr from dual)a,
                          (SELECT max(c.cfoffr) AS cfoffr,
                                  c.cfaccn
                           FROM   sync_cdc_cfoffl c
                           GROUP BY c.cfaccn) b
                   WHERE  a.orchestrate_cfcifn = b.cfaccn(+)
                   AND    TRIM(orchestrate_cfcifn) IS NOT NULL
                   AND    orchestrate_cfcifn <> 0) src
            ON (c.cif_no = src.cfcifn)
            WHEN MATCHED THEN
              UPDATE
              SET    c.cif_name      = src.cfsnme,
                     c.business_type = src.cfbust,
                     c.office_code   = src.cfoffr

            WHEN NOT MATCHED THEN
              INSERT
                (c.cif_no,
                 c.cif_name,
                 c.business_type,
                 c.office_code)
              VALUES
                (src.cfcifn,
                 src.cfsnme,
                 src.cfbust,
                 src.cfoffr);
            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFTNEW', 'BK_FEE_DISCOUNT',
                                                    orchestrate_cfcifn, dm_operation_type, l_err_desc);
      CSPKS_CDC_T24_UTIL.PR_T24_CDD_LOG_CFTNEW ( dm_operation_type  , orchestrate_cfcifn  ,  orchestrate_cfsnme  ,
                                      orchestrate_cfbust  , orchestrate_cfoffr  , 0 , -1 ,  l_err_desc ) ;
            COMMIT;
            RAISE;

    END;
    PROCEDURE pr_cfoffl_sync(dm_operation_type in CHAR,
                                    orchestrate_cfaccn in NUMBER,
                                    orchestrate_cfatyp in CHAR,
                                    orchestrate_cfoffr in VARCHAR)
    IS
    l_err_desc VARCHAR2(250);
    l_cfoffl VARCHAR2(20);
    BEGIN
        IF dm_operation_type <> 'D' THEN
            MERGE INTO sync_cdc_cfoffl c
             USING (SELECT orchestrate_cfaccn AS cfaccn,
                           orchestrate_cfatyp AS cfatyp,
                           orchestrate_cfoffr AS cfoffr
                      FROM DUAL) a
                ON (c.cfaccn = a.cfaccn
                AND c.cfoffr = a.cfoffr)
            WHEN MATCHED
            THEN
                UPDATE SET c.cfatyp = a.cfatyp
            WHEN NOT MATCHED
            THEN
                INSERT (c.cfaccn,
                        c.cfatyp,
                        c.cfoffr)
                VALUES (a.cfaccn,
                        a.cfatyp,
                        a.cfoffr); --HAONS 20150324 CHANGES

                /*INSERT INTO sync_cdc_cfoffl c
                        (c.cfaccn,
                        c.cfatyp,
                        c.cfoffr)
                VALUES (orchestrate_cfaccn,
                        orchestrate_cfatyp,
                        orchestrate_cfoffr);*/

                SELECT MAX (d.cfoffr)
                  INTO l_cfoffl
                  FROM sync_cdc_cfoffl d
                 WHERE d.cfaccn = orchestrate_cfaccn;

                UPDATE bk_fee_discount a
                   SET a.office_code = l_cfoffl
                 WHERE a.cif_no = orchestrate_cfaccn;
        COMMIT;
        END IF;

        EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);
            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFOFFL', 'BK_FEE_DISCOUNT',
                                                    orchestrate_cfaccn, dm_operation_type, l_err_desc);

        COMMIT;
        RAISE;

    END;

END;

/
