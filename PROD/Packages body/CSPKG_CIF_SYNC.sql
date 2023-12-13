--------------------------------------------------------
--  DDL for Package Body CSPKG_CIF_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."CSPKG_CIF_SYNC" 
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
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_cfaddr_sync ( dm_operation_type in CHAR,
                            orchestrate_cfcifn in NUMBER,
                            orchestrate_cfadsq in VARCHAR2,
                            orchestrate_cfna2 in VARCHAR2 )
    IS
        l_cifno VARCHAR2(30);
        l_err_desc VARCHAR2(250);
        l_taget_table VARCHAR2(100);
    BEGIN
        IF dm_operation_type <> 'D' THEN
            l_cifno := TRIM(orchestrate_cfcifn);

            l_taget_table := 'BK_CIF';
            UPDATE bk_cif c SET  c.addr =  TRIM(orchestrate_cfna2)
            WHERE c.cif_no = l_cifno;

            l_taget_table := 'BB_CORP_INFO';

            UPDATE bb_corp_info c SET  c.address =  TRIM(orchestrate_cfna2)
            WHERE TO_NUMBER(c.cif_no) = TO_NUMBER(l_cifno)
            AND c.status = 'ACTV';

            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFADDR', l_taget_table,
                                                    orchestrate_cfcifn, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_cfmast_sync ( dm_operation_type in CHAR,
                                orchestrate_cfcifn in NUMBER,
                                orchestrate_cfsscd in VARCHAR2,
                                orchestrate_cfssno in VARCHAR2,
                                orchestrate_cfbrnn in NUMBER,
                                orchestrate_cfna1 in VARCHAR2,
                                orchestrate_cfbird in NUMBER,
                                orchestrate_cfbirp in VARCHAR2,
                                orchestrate_cfcitz in VARCHAR2,
                                orchestrate_cfindi in VARCHAR2,
                                orchestrate_taxcod in VARCHAR2)
    IS
    l_taget_table VARCHAR2(100);
    l_err_desc VARCHAR2(250);
    BEGIN
        IF dm_operation_type <> 'D' AND LENGTH(RTRIM(orchestrate_cfssno)) < 40 THEN
            --UPDATE BK_CIF
            MERGE INTO bk_cif c
            USING (SELECT
                    TRIM(orchestrate_cfcifn) AS cfcifn,
                    TRIM(orchestrate_cfsscd) AS cfsscd,
                    TRIM(orchestrate_cfssno) AS cfssno,
                    LPAD(TRIM(orchestrate_cfbrnn), 3,'0') AS cfbrnn,
                    TRIM(orchestrate_cfna1) AS cfna1,
                    DECODE(LENGTH(orchestrate_cfbird),7,
                           TO_DATE(orchestrate_cfbird,'yyyyddd'),NULL) AS cfbird,
                    TRIM(orchestrate_cfbirp) AS cfbirp,
                    TRIM(orchestrate_cfcitz) AS cfcitz,
                    TRIM(orchestrate_cfindi) AS cfindi,
                    TRIM(orchestrate_taxcod) AS taxcod--,
                    --b.cfna2 AS cfna2
                   FROM DUAL b
                   ) a
            ON (a.cfcifn = c.cif_no)
            WHEN MATCHED THEN
              UPDATE
              SET    c.cert_type     = a.cfsscd,
                     c.cert_code     = a.cfssno,
                     c.bank_no       = '302',
                     c.org_no        = a.cfbrnn,
                     c.cif_acct_name = a.cfna1,
                     c.birth_date    = a.cfbird,
                     c.birth_place   = a.cfbirp,
                     c.country       = a.cfcitz,
                     c.individual    = a.cfindi,
                     c.taxcod        = a.taxcod--,
                     --c.addr          = a.cfna2
                    ;

            --UPDATE bb_corp_info
            MERGE INTO   bb_corp_info c
                     USING   (SELECT
                                        orchestrate_cfcifn AS cfcifn,
                                        orchestrate_cfsscd AS cfsscd,
                                        orchestrate_cfssno AS cfssno,
                                        orchestrate_cfbrnn AS cfbrnn,
                                        orchestrate_cfna1 AS cfna1,
                                        orchestrate_cfbird AS cfbird,
                                        orchestrate_cfbirp AS cfbirp,
                                        orchestrate_cfcitz AS cfcitz,
                                        orchestrate_cfindi AS cfindi,
                                        orchestrate_taxcod AS taxcod--,
                                       --b.cfna2
                                FROM DUAL b
                             ) a
                        ON   (a.cfcifn = c.cif_no AND c.status = 'ACTV')
                WHEN MATCHED
                THEN
                    UPDATE SET c.cert_type = TRIM (a.cfsscd),
                               c.cert_code = TRIM (a.cfssno),
                               c.cif_acct_name = TRIM (a.cfna1)--,
                               --c.address = a.cfna2
                               ;

            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFMAST', l_taget_table,
                                                    orchestrate_cfcifn, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;

  END;

 /*----------------------------------------------------------------------------------------------------
 ** Description:
 **  for syn cfconn
 **  Person      Date           Comments
 **  HaoNS     04/11/2014    Created
 **  LocTX     12/11/2014    modify for clear

----------------------------------------------------------------------------------------------------*/


    PROCEDURE pr_all_address_cif_sync( dm_operation_type IN CHAR,
                                    orchestrate_cfaccn IN NUMBER,
                                    orchestrate_cfeadd IN VARCHAR2,
                                    orchestrate_cfeadc IN VARCHAR2,
                                    orchestrate_cfatyp IN CHAR)
    IS
        l_cif_no  VARCHAR2(30);
        l_address VARCHAR2(100);
        l_taget_table VARCHAR2(100);
        l_err_desc VARCHAR2(250);
        l_check NUMBER;
    BEGIN
        l_address := TRIM(orchestrate_cfeadd);

        IF dm_operation_type <> 'D' AND orchestrate_cfeadc IN ('EM','MP')
                               AND orchestrate_cfatyp = 'C'
                               AND l_address IS NOT NULL THEN

            l_cif_no := TRIM(orchestrate_cfaccn);

            IF orchestrate_cfeadc = 'EM' THEN
                l_taget_table := 'BK_CIF_EM';
                --bk_cif
                UPDATE bk_cif c SET c.email = l_address
                WHERE c.cif_no = l_cif_no;

                --bb_corp_info
                l_taget_table := 'BB_CORP_INFO_EM';
                UPDATE bb_corp_info c SET c.email = l_address
                WHERE c.cif_no = l_cif_no
                AND c.status = 'ACTV';

                l_check := sql%rowcount;

                --bb_user_info
                /*--#20150608 LocTX disable theo y/c cua a Ngoc, ko cap nhat thong cua KHDN vao thong tin lien lac cua user
                IF l_check > 0 THEN--#20150526 LocTX add CHECK for tunining
                    l_taget_table := 'BB_USER_INFO_EM';
                    MERGE INTO   bb_user_info c
                    USING   (SELECT   l_address addr,
                                       b.corp_id
                                FROM   bb_corp_info b
                               WHERE    l_cif_no = b.cif_no
                             )
                             a
                    ON   (a.corp_id = c.corp_id AND c.status = 'ACTV')
                    WHEN MATCHED
                    THEN
                        UPDATE SET c.email = a.addr;

                END IF;
                */

                --bc_user_info
--                IF l_check = 0 THEN--#20150526 LocTX add for tunining
--                    l_taget_table := 'BC_USER_INFO_EM';
--
--                    UPDATE bc_user_info c SET c.email = l_address
--                    WHERE c.cif_no = l_cif_no;
--                END IF;


            END IF;

            IF orchestrate_cfeadc = 'MP' AND LENGTH(RTRIM(orchestrate_cfeadd)) < 21 THEN

                --bk_cif
                l_taget_table := 'BK_CIF_MP';
                UPDATE bk_cif c SET c.mobile = l_address
                WHERE c.cif_no = l_cif_no;

                --bb_corp_info
                l_taget_table := 'BB_CORP_INFO_MP';
                UPDATE bb_corp_info c SET c.mobile = l_address
                WHERE c.cif_no = l_cif_no
                AND c.status = 'ACTV';

                l_check := sql%rowcount;

                --bb_user_info
                --#20150608 LocTX disable theo y/c cua a Ngoc, ko cap nhat thong cua KHDN vao thong tin lien lac cua user
                /*
                IF l_check > 0 THEN--#20150526 LocTX add CHECK for tunining
                     l_taget_table := 'BB_USER_INFO_MP';
                    MERGE INTO   bb_user_info c
                         USING   (SELECT   l_address addr,
                                           b.corp_id
                                    FROM   bb_corp_info b
                                   WHERE    l_cif_no = b.cif_no
                                 )
                                 a
                            ON   (a.corp_id = c.corp_id AND c.status = 'ACTV')
                    WHEN MATCHED
                    THEN
                        UPDATE SET c.mobile = a.addr;
                END IF;
                */

                --bc_user_info
--                IF l_check = 0 THEN--#20150526 LocTX add for tunining
--                    l_taget_table := 'BC_USER_INFO_MP';
--                    UPDATE bc_user_info c SET c.mobile = l_address
--                    WHERE c.cif_no = l_cif_no and 1=2;  --- update CA 1646 4/04/2019
--                END IF;

            END IF;

            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFCONN', l_taget_table,
                                                    orchestrate_cfaccn, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
    PROCEDURE pr_cftnam_sync(dm_operation_type IN CHAR,
                            orchestrate_cttcif IN  NUMBER ,
                            orchestrate_ctname IN VARCHAR,
                            orchestrate_ctadd1 IN VARCHAR,
                            orchestrate_ctadd2 IN VARCHAR,
                            orchestrate_ctssno IN VARCHAR,
                            orchestrate_ctsscd IN VARCHAR,
                            orchestrate_ctsnme IN VARCHAR,
                            orchestrate_cthpho IN VARCHAR,
                            orchestrate_ctbpho IN VARCHAR,
                            orchestrate_ctbfho IN VARCHAR,
                            orchestrate_ctcoun IN VARCHAR,
                            orchestrate_ctpur6 IN NUMBER ,
                            orchestrate_ctpur7 IN NUMBER ,
                            orchestrate_ctidi6 IN NUMBER ,
                            orchestrate_ctidi7 IN NUMBER ,
                            orchestrate_ctista IN VARCHAR,
                            orchestrate_ctiplc IN VARCHAR,
                            orchestrate_ctirm1 IN VARCHAR,
                            orchestrate_ctirm2 IN VARCHAR,
                            orchestrate_ctirm3 IN VARCHAR)
    IS
     l_err_desc VARCHAR2(250);
    BEGIN
        IF dm_operation_type <> 'D' THEN
            MERGE INTO sync_cdc_cftnam c
            USING(SELECT orchestrate_cttcif AS cttcif ,
                    orchestrate_ctname AS ctname,
                    orchestrate_ctadd1 AS ctadd1,
                    orchestrate_ctadd2 AS ctadd2,
                    orchestrate_ctssno AS ctssno,
                    orchestrate_ctsscd AS ctsscd,
                    orchestrate_ctsnme AS ctsnme,
                    orchestrate_cthpho AS cthpho,
                    orchestrate_ctbpho AS ctbpho,
                    orchestrate_ctbfho AS ctbfho,
                    orchestrate_ctcoun AS ctcoun,
                    orchestrate_ctpur6 AS ctpur6,
                    orchestrate_ctpur7 AS ctpur7,
                    orchestrate_ctidi6 AS ctidi6,
                    orchestrate_ctidi7 AS ctidi7,
                    orchestrate_ctista AS ctista,
                    orchestrate_ctiplc AS ctiplc,
                    orchestrate_ctirm1 AS ctirm1,
                    orchestrate_ctirm2 AS ctirm2,
                    orchestrate_ctirm3 AS ctirm3
                    FROM DUAL )a
                    ON(c.cttcif = a.cttcif)
            WHEN MATCHED THEN
                UPDATE
                SET c.ctname = a.ctname,
                    c.ctadd1 = a.ctadd1,
                    c.ctadd2 = a.ctadd2,
                    c.ctssno = a.ctssno,
                    c.ctsscd = a.ctsscd,
                    c.ctsnme = a.ctsnme,
                    c.cthpho = a.cthpho,
                    c.ctbpho = a.ctbpho,
                    c.ctbfho = a.ctbfho,
                    c.ctcoun = a.ctcoun,
                    c.ctpur6 = a.ctpur6,
                    c.ctpur7 = a.ctpur7,
                    c.ctidi6 = a.ctidi6,
                    c.ctidi7 = a.ctidi7,
                    c.ctista = a.ctista,
                    c.ctiplc = a.ctiplc,
                    c.ctirm1 = a.ctirm1,
                    c.ctirm2 = a.ctirm2,
                    c.ctirm3 = a.ctirm3
            WHEN NOT MATCHED THEN
                INSERT(c.cttcif,
                        c.ctname,
                        c.ctadd1,
                        c.ctadd2,
                        c.ctssno,
                        c.ctsscd,
                        c.ctsnme,
                        c.cthpho,
                        c.ctbpho,
                        c.ctbfho,
                        c.ctcoun,
                        c.ctpur6,
                        c.ctpur7,
                        c.ctidi6,
                        c.ctidi7,
                        c.ctista,
                        c.ctiplc,
                        c.ctirm1,
                        c.ctirm2,
                        c.ctirm3 )
                VALUES(a.cttcif,
                        a.ctname,
                        a.ctadd1,
                        a.ctadd2,
                        a.ctssno,
                        a.ctsscd,
                        a.ctsnme,
                        a.cthpho,
                        a.ctbpho,
                        a.ctbfho,
                        a.ctcoun,
                        a.ctpur6,
                        a.ctpur7,
                        a.ctidi6,
                        a.ctidi7,
                        a.ctista,
                        a.ctiplc,
                        a.ctirm1,
                        a.ctirm2,
                        a.ctirm3 );
        END IF;
     COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFTNAM', 'SYNC_CDC_CFTNAM',
                                                    orchestrate_cttcif, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
END;

/
