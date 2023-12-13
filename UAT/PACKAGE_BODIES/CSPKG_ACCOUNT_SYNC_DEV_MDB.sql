--------------------------------------------------------
--  DDL for Package Body CSPKG_ACCOUNT_SYNC_DEV_MDB
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."CSPKG_ACCOUNT_SYNC_DEV_MDB" 
/* Formatted on 09-Oct-2014 12:35:16 (QP5 v5.160) */
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

   -- pkgctx    plog.log_ctx;
  --  logrow    tlogdebug%ROWTYPE;

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created
 **  DuongDV    05/12/2014    Edit: them phan dong bo toi SYN_DDMEMO_CDC
----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_ddmast_sync (dm_operation_type in CHAR,
                            orchestrate_before_status IN NUMBER,
                            orchestrate_before_acname  IN VARCHAR2,
                            orchestrate_before_cifno IN NUMBER,
                            orchestrate_before_branch IN NUMBER,
                            orchestrate_acctno IN NUMBER,
                            orchestrate_status IN NUMBER,
                            orchestrate_acname IN VARCHAR2,
                            orchestrate_cifno IN NUMBER,
                            orchestrate_branch IN NUMBER
                            )
    IS
        l_acc_status VARCHAR2 (10);
        l_acc_no VARCHAR2(14) ;
        l_err_desc VARCHAR2(250);

    BEGIN
        IF dm_operation_type = 'U' AND TRIM(orchestrate_status) IS NOT NULL
        AND (
            orchestrate_before_status <> orchestrate_status
            OR orchestrate_before_acname <> orchestrate_acname
            OR orchestrate_before_cifno <> orchestrate_cifno
            OR orchestrate_before_branch <> orchestrate_branch
        )
        THEN
            l_acc_status := fn_get_account_status_code (orchestrate_status);
                l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);

            UPDATE bk_account_info c
            SET c.status = l_acc_status,
                c.acct_name = TRIM(orchestrate_acname),
                c.cif_no = orchestrate_cifno,
                c.branch_no = (case when length(orchestrate_branch) <= 2 then LPAD (orchestrate_branch, 3, '0') else TO_CHAR(orchestrate_branch) end)
            WHERE c.acct_no = l_acc_no;

            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'DDMAST', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;



    PROCEDURE pr_cdmast_sync (dm_operation_type in CHAR,
                            orchestrate_before_status in NUMBER,
                            orchestrate_before_acname IN VARCHAR2,
                            --orchestrate_before_type IN VARCHAR2,
                            --orchestrate_before_brn IN NUMBER,
                            --orchestrate_before_cifno IN NUMBER,
                            --orchestrate_before_hold IN NUMBER,
                            --orchestrate_before_cdnum IN NUMBER,
                            --orchestrate_before_rate IN NUMBER,

                            orchestrate_acctno IN NUMBER,
                            orchestrate_status IN NUMBER,
                            orchestrate_acname IN VARCHAR2,
                            orchestrate_cdtcod IN CHAR--,
                            --orchestrate_type IN VARCHAR2,
                            --orchestrate_brn IN NUMBER,
                            --orchestrate_cifno IN NUMBER,
                            --orchestrate_hold IN NUMBER,
                            --orchestrate_cdnum IN NUMBER,
                            --orchestrate_rate IN NUMBER
                            )
    IS
        l_acc_status VARCHAR2 (10);
        l_acc_no VARCHAR2(14) ;
        l_err_desc VARCHAR2(250);

    BEGIN
        IF dm_operation_type = 'U' AND TRIM(orchestrate_status) IS NOT NULL
        AND (
            orchestrate_before_status <> orchestrate_status
            OR orchestrate_before_acname <> orchestrate_acname
            --OR orchestrate_before_type <> orchestrate_type
            --OR orchestrate_before_cifno <> orchestrate_cifno
            --OR orchestrate_before_brn <> orchestrate_brn
            --OR orchestrate_before_hold <> orchestrate_hold
            --OR orchestrate_before_cdnum <> orchestrate_cdnum
            --OR orchestrate_before_rate <> orchestrate_rate
        )
        THEN
            l_acc_status := fn_get_account_status_code (orchestrate_status);

            l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);

            UPDATE bk_receipt_info c SET
                    /*
                    c.product_code = TRIM(orchestrate_type),
                    c.interest_rate = orchestrate_rate,

                    c.term = a.cdterm,
                    c.is_rollout_interest = a.renew,
                    c.interest_receive_account = a.dactn,
                    --c.account_no = orchestrate_cdnum,
                    */
                    c.status = l_acc_status,
                    c.cdtcod = orchestrate_cdtcod
            WHERE receipt_no = l_acc_no;


            UPDATE bk_account_info c
            SET c.status = l_acc_status,
                c.acct_name = TRIM(orchestrate_acname)--,
                /*c.cif_no = orchestrate_cifno,
                c.branch_no =  LPAD (orchestrate_brn, 3, '0'),
                c.hold_amount = orchestrate_hold,
                c.p_acct_no = LPAD (orchestrate_cdnum, 14, '0'),
                c.product_type = TRIM(orchestrate_type),
                c.interest_rate=  orchestrate_rate
                */
            WHERE c.acct_no = l_acc_no;


            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CDMAST', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;



    PROCEDURE pr_lnmast_sync (dm_operation_type in CHAR,
                            orchestrate_before_status in NUMBER,
                            --orchestrate_before_acname IN VARCHAR2,
                            orchestrate_before_type IN VARCHAR2,
                            --orchestrate_before_brn IN NUMBER,
                            --orchestrate_before_cifno IN NUMBER,
                            orchestrate_before_orgamt IN NUMBER,
                            orchestrate_before_term IN NUMBER,
                            orchestrate_before_tmcode IN VARCHAR2,
                            orchestrate_before_pmtamt IN NUMBER,
                            orchestrate_before_fnlpmt IN NUMBER,
                            --orchestrate_before_offcr VARCHAR2,
                            orchestrate_before_rate IN NUMBER,
                            orchestrate_acctno IN NUMBER,
                            orchestrate_status IN NUMBER,
                            --orchestrate_acname IN VARCHAR2,
                            orchestrate_type IN VARCHAR2,
                            --orchestrate_brn IN NUMBER,
                            --orchestrate_cifno IN NUMBER,
                            orchestrate_orgamt IN NUMBER,
                            orchestrate_term IN NUMBER,
                            orchestrate_tmcode IN VARCHAR2,
                            orchestrate_pmtamt IN NUMBER,
                            orchestrate_fnlpmt IN NUMBER,
                            --orchestrate_offcr VARCHAR2,
                            orchestrate_rate IN NUMBER
                            )
    IS
        l_acc_status VARCHAR2 (10);
        l_acc_no VARCHAR2(14) ;
        l_err_desc VARCHAR2(250);

    BEGIN
        IF dm_operation_type = 'U' AND TRIM(orchestrate_status) IS NOT NULL
        AND (
            orchestrate_before_status <> orchestrate_status
            --OR orchestrate_before_acname <> orchestrate_acname
            OR orchestrate_before_type <> orchestrate_type
            --OR orchestrate_before_cifno <> orchestrate_cifno
            --OR orchestrate_before_brn <> orchestrate_brn
            OR orchestrate_before_rate <> orchestrate_rate
            OR orchestrate_before_orgamt <> orchestrate_orgamt
            OR orchestrate_before_term <> orchestrate_term
            OR orchestrate_before_tmcode <> orchestrate_tmcode
            OR orchestrate_before_pmtamt   <> orchestrate_pmtamt
            OR orchestrate_before_fnlpmt <> orchestrate_fnlpmt
        )
        THEN
            l_acc_status := fn_get_account_status_code (orchestrate_status);

            l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);

            UPDATE bk_account_info c
            SET c.status = l_acc_status,
                --c.cif_no = orchestrate_cifno,
                --c.branch_no =  LPAD (orchestrate_brn, 3, '0'),
                c.product_type = TRIM(orchestrate_type),
                c.interest_rate =  orchestrate_rate,
                        c.payment_amount = orchestrate_pmtamt,
                        c.final_payment_amount = orchestrate_fnlpmt,
                        c.original_balance = orchestrate_orgamt,
                        c.remark           = orchestrate_TERM || orchestrate_tmcode
            WHERE c.acct_no = l_acc_no;

            COMMIT;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'LNMAST', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
        PROCEDURE pr_ddmemo_sync (dm_operation_type in CHAR,
                                orchestrate_acctno IN NUMBER,
                              orchestrate_status IN NUMBER,
                              orchestrate_hold IN NUMBER,
                              orchestrate_cbal IN NUMBER,
                              orchestrate_odlimt IN NUMBER,
                              orchestrate_acname IN VARCHAR2,
                              orchestrate_dla7 IN NUMBER)
    IS
        l_acc_status VARCHAR2 (10);
        l_acc_no VARCHAR2(14) ;
        l_err_desc VARCHAR2(250);

    BEGIN
        IF dm_operation_type <> 'D' THEN
            IF TRIM (orchestrate_status) IS NOT NULL THEN
                l_acc_status := fn_get_account_status_code (orchestrate_status);

                l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);

                UPDATE bk_account_info c
                   SET c.status = l_acc_status,
                       c.hold_amount = orchestrate_hold,
                       c.ledger_balance = orchestrate_cbal,
                       c.available_balance = orchestrate_cbal
                           - orchestrate_hold
                           + orchestrate_odlimt,
                       c.overdraft_limit = orchestrate_odlimt,
                       c.acct_name = TRIM (orchestrate_acname)
                WHERE c.acct_no = l_acc_no;

                COMMIT;

            END IF;


        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'DDMEMO', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;

    END;

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created
 **  LocTX      16/06/2015      LocTX chuyen doan check user dang ky ebank xuong duoi: muc dich; dong bo tat ca danh muc TK tu core

----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_ddtnew_sync (dm_operation_type IN CHAR,
                              orchestrate_branch IN NUMBER,
                              orchestrate_acctno IN NUMBER,
                              orchestrate_actype IN VARCHAR,
                              orchestrate_ddctyp IN VARCHAR,
                              orchestrate_cifno IN NUMBER,
                              orchestrate_status IN NUMBER,
                              orchestrate_hold IN NUMBER,
                              orchestrate_cbal IN NUMBER,
                              orchestrate_odlimt IN NUMBER,
                              orchestrate_rate IN NUMBER,
                              orchestrate_acname IN VARCHAR,
                              orchestrate_sccode IN VARCHAR,
                              orchestrate_datop7 IN NUMBER,
                              orchestrate_accrue IN NUMBER)
    IS
        l_acct_status   VARCHAR2 (10);
        l_acct_no VARCHAR2(14);
        l_acct_type VARCHAR2(10);
        l_err_desc VARCHAR2(250);
        l_check NUMBER;

    BEGIN

        /*
        --#20150701 LocTX add tam thoi: loai bo san pham MDB
        SELECT (CASE
                    WHEN EXISTS (   SELECT 1
                                    FROM ibs.cstb_branch_mdb_loai_tk
                                    WHERE branch_code = orchestrate_branch )
                    THEN 1
                    ELSE 0
                END)
        INTO l_check
        FROM DUAL;

        IF l_check > 0 THEN
            RETURN;
        END IF;
        --#20150701 LocTX end
        */


        --#20141202 Loctx add for benifit info
        IF dm_operation_type <> 'D' THEN
            MERGE INTO cstb_account_info c
            USING (SELECT orchestrate_cifno cif_no,
                        orchestrate_acctno acct_no,
                        orchestrate_acname ac_name
                    FROM DUAL
                    ) A
            ON (c.acct_no =  a.acct_no AND c.module = 'DD')
            WHEN MATCHED THEN
                UPDATE SET c.acct_name = a.ac_name, c.cif_no = a.cif_no
            WHEN NOT MATCHED THEN
                INSERT  (module, cif_no, acct_no, acct_name)
                    VALUES ( 'DD', a.cif_no, a.acct_no, a.ac_name)
                    ;

        END IF;


        IF dm_operation_type <> 'D'
            AND TRIM (orchestrate_status) IS NOT NULL
            AND orchestrate_status <> 2
            AND orchestrate_acctno <> 0
            AND orchestrate_sccode <> 'CA12OPI'
        THEN

            l_acct_status := fn_get_account_status_code (orchestrate_status);
            l_acct_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);
            l_acct_type := fn_get_actype_code (orchestrate_actype);

/*--#20150617 LocTX move xuong duoi
            --#20150327 Loctx check if khach hang da dang ky ebank
            SELECT CASE
                       WHEN EXISTS
                                (SELECT 1
                                   FROM (SELECT cif_no FROM bc_user_info
                                         UNION
                                         SELECT cif_no FROM bb_corp_info) a
                                  WHERE a.cif_no = TO_CHAR (orchestrate_cifno))
                       THEN 1
                       ELSE 0
                   END
              INTO l_check
              FROM DUAL;

            IF l_check = 1
            THEN
*/
                MERGE INTO   bk_account_info c
                USING(SELECT
                              (case when length(orchestrate_branch) <= 2 then LPAD (orchestrate_branch, 3, '0') else TO_CHAR(orchestrate_branch) end) AS branch,
                              l_acct_no acctno,
                              l_acct_type actype,
                              orchestrate_ddctyp AS ddctyp,
                              orchestrate_cifno AS cifno,
                              l_acct_status status,
                              TO_DATE (orchestrate_datop7, 'yyyyddd') AS issued_date,
                              orchestrate_hold AS hold,
                              orchestrate_cbal AS cbal,
                              orchestrate_odlimt AS odlimt,
                              TRIM(orchestrate_acname) acname,
                              TRIM (orchestrate_sccode) product_type,
                              orchestrate_rate AS rate,
                              orchestrate_accrue AS accrue
                        FROM dual
                        WHERE SUBSTR(l_acct_no, 4, 2) <> '31'
                        ) src
                ON   (src.acctno = c.acct_no)
                WHEN MATCHED
                THEN
                    UPDATE SET
                    c.status = src.status,
                    c.interest_rate = src.rate,
                    c.accured_interest = src.accrue,
                    c.product_type = src.product_type,
                    c.acct_name = src.acname --#20141128 Loctx add  (fss bo sung)
                WHEN NOT MATCHED
                THEN
                    INSERT(c.bank_no,
                                    c.org_no,
                                    c.branch_no,
                                    c.acct_no,
                                    c.acct_type,
                                    c.currency_code,
                                    c.cif_no,
                                    c.status,
                                    c.establish_date,
                                    c.hold_amount,
                                    c.ledger_balance,
                                    c.available_balance,
                                    c.overdraft_limit,
                                    c.interest_rate,
                                    c.acct_name,
                                    c.product_type,
                                    c.issued_date,
                                    c.accured_interest)
                    VALUES   (c_bank_no,
                             src.branch,
                             src.branch,
                             src.acctno,
                             src.actype,
                             TRIM (src.ddctyp),
                             src.cifno,
                             src.status,
                             src.issued_date,
                             src.hold,
                             src.cbal,
                             src.cbal - src.hold,
                             src.odlimt,
                             src.rate,
                             TRIM (src.acname),
                             src.product_type,
                             src.issued_date,
                             src.accrue);

            --#20150327 Loctx check if khach hang da dang ky ebank
            SELECT CASE
                       WHEN EXISTS
                                (SELECT 1
                                   FROM (
                                         SELECT cif_no FROM bb_corp_info) a
                                  WHERE a.cif_no = TO_CHAR (orchestrate_cifno))
                       THEN 1
                       ELSE 0
                   END
              INTO l_check
              FROM DUAL;

--            IF l_check = 1--#20150617 LocTX move toi day
--            THEN
--               MERGE INTO bc_related_account c
--                    USING(SELECT
--                                l_acct_no acctno,
--                                l_acct_status status,
--                                l_acct_type actype,
--                                b.user_id
--                            FROM  bc_user_info b
--                            --WHERE TO_CHAR (trim(orchestrate_cifno)) = trim(b.cif_no)
--                            WHERE TO_CHAR (orchestrate_cifno) = trim(b.cif_no)--#20150121 Loctx change
--                            AND b.status = 'ACTV'
--                    )src
--                    ON(src.acctno = c.acct_no
--                    AND src.user_id = c.user_id)
--               WHEN MATCHED
--               THEN
--                   UPDATE SET c.status = src.status
--               WHEN NOT MATCHED
--               THEN
--                    INSERT(c.relation_id,
--                                c.user_id,
--                                c.acct_no,
--                                c.acct_type,
--                                c.is_master,
--                                c.status,
--                                c.create_time)
--                       VALUES(seq_relation_id.NEXTVAL,
--                                 src.user_id,
--                                 src.acctno,
--                                 src.actype,
--                                 'N',
--                                 src.status,
--                                 SYSDATE);
--            END IF;
        END IF;
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'DDTNEW', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;

    END;

/*----------------------------------------------------------------------------------------------------
 **  Description: Xu ly truong hop so du cap nhat truoc khi tai khoan vao
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  ThanhNT     06/08/2015    Created
----------------------------------------------------------------------------------------------------*/

    FUNCTION  fn_cdmemo_process (
                                    dm_operation_type   IN CHAR,
                                    orchestrate_acctno  IN NUMBER,
                                    orchestrate_curtyp  IN VARCHAR,
                                    orchestrate_cbal    IN NUMBER,
                                    orchestrate_accint  IN NUMBER,
                                    orchestrate_penamt  IN NUMBER,
                                    orchestrate_hold    IN NUMBER,
                                    orchestrate_wdrwh   IN NUMBER,
                                    orchestrate_cdnum   IN NUMBER,
                                    orchestrate_status  IN NUMBER,
                                    p_first_process_ind IN CHAR
                                )
    RETURN NUMBER
    IS
        l_acc_no VARCHAR2(14);
        l_err_desc VARCHAR2(250);
        l_status VARCHAR(4);
        l_return NUMBER;
        l_account_no NUMBER(23);
        l_sucess NUMBER :=1;
    BEGIN

        IF dm_operation_type <> 'D'
                    AND TRIM(orchestrate_status) IS NOT NULL
                    AND orchestrate_acctno <> 0
        THEN
            l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);
            l_status := fn_get_account_status_code(orchestrate_status);

            SELECT  NVL(MAX(acct_no),0)
            INTO    l_account_no
            FROM
            (
                   ( SELECT  acct_no
                    FROM    bk_account_info
                    WHERE   acct_no = l_acc_no
                    AND     acct_type = 'FD'
                    )
                UNION
                    ( SELECT receipt_no AS acct_no
                      FROM  bk_receipt_info
                      WHERE receipt_no = l_acc_no
                    )
             );

            --Check tai khoan co trong bang bk_account_info chua
            IF  l_account_no = 0
            THEN
                IF p_first_process_ind = 'Y'
                THEN
                    INSERT INTO cstb_data_later_process
                    (
                        insert_date7,
                        insert_date,
                        table_name,
                        store_procedure,
                        process_ind
                    )
                    VALUES
                    (
                        TO_CHAR(SYSDATE,'yyyyddd'),
                        SYSDATE,
                        'CDMEMO',
                        'cspkg_account_sync.fn_cdmemo_process ('''
                                                                            || dm_operation_type || ''','
                                                                            || orchestrate_acctno || ','''
                                                                            || TRIM(orchestrate_curtyp) || ''','
                                                                            || orchestrate_cbal || ','
                                                                            || orchestrate_accint || ','
                                                                            || orchestrate_penamt || ','
                                                                            || orchestrate_hold || ','
                                                                            || orchestrate_wdrwh || ','
                                                                            || orchestrate_cdnum || ','
                                                                            || orchestrate_status || ',
                                                                            ''N'')',
                        'N'
                    );
                    COMMIT;

                    RETURN cspkg_errnums.c_process_later;
                END IF;
                RETURN cspkg_errnums.c_process_later;
            END IF;

               MERGE INTO bk_receipt_info c
               USING(SELECT orchestrate_cbal AS cbal,
                            orchestrate_accint AS accint,
                            l_status AS status,
                            l_acc_no AS acctno,
                            'A' AS product_code
                            FROM dual) a
                ON (c.receipt_no = a.acctno)
                WHEN MATCHED
                THEN
                UPDATE SET c.principal = a.cbal,
                     c.interest_amount = a.accint,
                     c.status = a.status;

               MERGE INTO bk_account_info c
               USING(SELECT l_acc_no AS acctno,
                             TRIM(orchestrate_curtyp) AS curtyp,
                             orchestrate_cbal AS cbal,
                             orchestrate_accint AS accint,
                             orchestrate_penamt AS penamt,
                             orchestrate_hold AS hold,
                             orchestrate_wdrwh AS wdrwh,
                             l_status AS status ,
                             orchestrate_cdnum AS cdnum,
                             -1 AS cifno,
                             'NA' AS currency_code
                             FROM dual) a
                ON (c.acct_no = a.acctno)
                WHEN MATCHED
                THEN
                UPDATE SET
                    c.original_balance = a.cbal,
                    c.principal_balance  = a.cbal,
                    c.accured_interest   = a.accint,
                    c.penalty_amount     = a.penamt,
                    c.hold_amount        = a.hold,
                    c.current_cash_value = (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
                    c.status = a.status,
                    c.acct_type = 'FD',
                    c.p_acct_no = a.cdnum;

            COMMIT;
        END IF;
        RETURN cspkg_errnums.c_success;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CDMEMO', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
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

    PROCEDURE pr_cdmemo_sync (dm_operation_type in CHAR,
                            orchestrate_acctno in NUMBER,
                            orchestrate_curtyp in VARCHAR,
                            orchestrate_cbal in NUMBER,
                            orchestrate_accint in NUMBER,
                            orchestrate_penamt in NUMBER,
                            orchestrate_hold in NUMBER,
                            orchestrate_wdrwh in NUMBER,
                            orchestrate_cdnum in NUMBER,
                            orchestrate_status in NUMBER)
    IS
        l_acc_no VARCHAR2(14);
        l_err_desc VARCHAR2(250);
        l_status VARCHAR(4);
        l_return NUMBER;
    BEGIN
        l_return := fn_cdmemo_process   (
                                            dm_operation_type,
                                            orchestrate_acctno,
                                            orchestrate_curtyp,
                                            orchestrate_cbal,
                                            orchestrate_accint,
                                            orchestrate_penamt,
                                            orchestrate_hold,
                                            orchestrate_wdrwh,
                                            orchestrate_cdnum,
                                            orchestrate_status,
                                            'Y'
                                        );

/*
        IF dm_operation_type <> 'D'
                    AND TRIM(orchestrate_status) IS NOT NULL
                    AND orchestrate_acctno <> 0
        THEN
            l_acc_no := LPAD (orchestrate_acctno, 14, '0');
            l_status := fn_get_account_status_code(orchestrate_status);


               MERGE INTO bk_receipt_info c
               USING(SELECT orchestrate_cbal AS cbal,
                            orchestrate_accint AS accint,
                            l_status AS status,
                            l_acc_no AS acctno,
                            'A' AS product_code
                            FROM dual) a
                ON (c.receipt_no = a.acctno)
                WHEN MATCHED
                THEN
                UPDATE SET c.principal = a.cbal,
                     c.interest_amount = a.accint,
                     c.status = a.status;
               MERGE INTO bk_account_info c
               USING(SELECT l_acc_no AS acctno,
                             TRIM(orchestrate_curtyp) AS curtyp,
                             orchestrate_cbal AS cbal,
                             orchestrate_accint AS accint,
                             orchestrate_penamt AS penamt,
                             orchestrate_hold AS hold,
                             orchestrate_wdrwh AS wdrwh,
                             l_status AS status ,
                             orchestrate_cdnum AS cdnum,
                             -1 AS cifno,
                             'NA' AS currency_code
                             FROM dual) a
                ON (c.acct_no = a.acctno)
                WHEN MATCHED
                THEN
                UPDATE SET
                    c.original_balance = a.cbal,
                    c.principal_balance  = a.cbal,
                    c.accured_interest   = a.accint,
                    c.penalty_amount     = a.penamt,
                    c.hold_amount        = a.hold,
                    c.current_cash_value = (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
                    c.status = a.status,
                    c.acct_type = 'FD',
                    c.p_acct_no = a.cdnum;

            COMMIT;
        END IF;
*/

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CDMEMO', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
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

    PROCEDURE pr_cdtnew_sync( dm_operation_type in CHAR,
                        orchestrate_bankno in NUMBER,
                        orchestrate_brn in NUMBER,
                        orchestrate_curtyp in VARCHAR,
                        orchestrate_cifno in NUMBER,
                        orchestrate_orgbal in NUMBER,
                        orchestrate_cbal in NUMBER,
                        orchestrate_accint in NUMBER,
                        orchestrate_penamt in NUMBER,
                        orchestrate_hold in NUMBER,
                        orchestrate_wdrwh in NUMBER,
                        orchestrate_cdnum in NUMBER,
                        orchestrate_issdt in NUMBER,
                        orchestrate_matdt in NUMBER,
                        orchestrate_rnwctr in NUMBER,
                        orchestrate_status in NUMBER,
                        orchestrate_acname in VARCHAR,
                        orchestrate_acctno in NUMBER,
                        orchestrate_type in VARCHAR,
                        orchestrate_rate in NUMBER,
                        orchestrate_renew in VARCHAR,
                        orchestrate_dactn in NUMBER,
                        orchestrate_cdterm in NUMBER,
                        orchestrate_cdmuid in VARCHAR)
    IS
         l_status VARCHAR2(10);
         l_acc_no VARCHAR(14);
         l_cdnum VARCHAR2(30);
         l_err_desc VARCHAR2(250);
         l_check NUMBER(3);
         l_taget_table VARCHAR2(100);
         l_cbal_check NUMBER(20);

    BEGIN

        /*
        --#20150701 QuanPD add tam thoi: loai bo san pham MDB
        SELECT (CASE
                    WHEN EXISTS (   SELECT 1
                                    FROM ibs.cstb_branch_mdb_loai_tk
                                    WHERE branch_code = orchestrate_brn )
                    THEN 1
                    ELSE 0
                END)
        INTO l_check
        FROM DUAL;

        IF l_check > 0 THEN
            RETURN;
        END IF;
        --#20150701 QuanPD end
        */

        --#20141202 Loctx add for benifit info
        IF dm_operation_type <> 'D' THEN
            MERGE INTO cstb_account_info c
            USING (SELECT orchestrate_cifno cif_no,
                        orchestrate_acctno acct_no,
                        orchestrate_acname ac_name
                    FROM DUAL
                    ) A
            ON (c.acct_no =  a.acct_no AND c.module = 'CD')
            WHEN MATCHED THEN
                UPDATE SET c.acct_name = a.ac_name, c.cif_no = a.cif_no
            WHEN NOT MATCHED THEN
                INSERT  (module, cif_no, acct_no, acct_name)
                    VALUES ( 'CD', a.cif_no, a.acct_no, a.ac_name)
                    ;

        END IF;

        COMMIT;
        IF dm_operation_type <> 'D' THEN
            l_status := fn_get_account_status_code(orchestrate_status);
            l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);
            l_cdnum := (case when length(orchestrate_cdnum) = 13 then LPAD (orchestrate_cdnum, 14, '0') else TO_CHAR(orchestrate_cdnum) end);

            SELECT CASE WHEN EXISTS(SELECT 1 FROM (
                                                  SELECT cif_no FROM bb_corp_info) a
                                    WHERE a.cif_no = TO_CHAR(orchestrate_cifno))
                        THEN 1
                        ELSE 0
                    END
            INTO l_check FROM dual;


            IF l_check = 1 THEN
            l_taget_table := 'BK_RECEIPT_INFO';
                   MERGE INTO   bk_receipt_info c
                        USING   (SELECT
                                          l_acc_no AS acctno,
                                          orchestrate_cdterm AS cdterm,
                                          orchestrate_orgbal AS orgbal,
                                          orchestrate_accint AS accint,
                                          l_cdnum AS cdnum,
                                          orchestrate_issdt AS issdt,
                                          orchestrate_matdt AS matdt,
                                          l_status AS status,
                                          TRIM (orchestrate_type) product_type,
                                          TRIM (orchestrate_renew) renew,
                                          orchestrate_dactn AS dactn,
                                          orchestrate_rate AS rate ,
                                          orchestrate_cbal AS cbal
                                   FROM   DUAL
                                  WHERE TRIM (orchestrate_status) IS NOT NULL
                                          AND TRIM (orchestrate_type) IS NOT NULL
                                          AND orchestrate_status <> 2 --AND b.curtyp = 'VND'
                                          AND orchestrate_status <> 0
                                ) a
                           ON   (a.acctno = c.receipt_no)
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.receipt_no,
                                            c.product_code,
                                            c.account_no,
                                            c.principal,
                                            c.interest_rate,
                                            c.interest_amount,
                                            c.term,
                                            c.opening_date,
                                            c.settlement_date,
                                            c.is_rollout_interest,
                                            c.interest_receive_account,
                                            c.status)
                           VALUES   ( a.acctno,
                                     a.product_type,
                                     a.cdnum,
                                     a.cbal,
                                     a.rate,
                                     a.accint,
                                     a.cdterm,
                                     TO_DATE (a.issdt, 'yyyyddd'),
                                     TO_DATE (a.matdt, 'yyyyddd'),
                                     TRIM (a.renew),
                                     a.dactn,
                                     a.status);

                l_taget_table := 'BK_ACCOUNT_INFO';
         --COMMIT; --HAONS ADD 20150329
                   MERGE INTO   bk_account_info c
                        USING   (SELECT
                                          l_acc_no AS acctno,
                                          (case when length(orchestrate_brn) <= 2 then LPAD (orchestrate_brn, 3, '0') else TO_CHAR(orchestrate_brn) end) AS brn,
                                          orchestrate_cifno AS cifno,
                                          orchestrate_cdterm AS cdterm,
                                          trim(orchestrate_curtyp) AS curtyp,
                                          orchestrate_cbal  AS cbal,
                                          orchestrate_orgbal AS orgbal,
                                          orchestrate_accint AS accint,
                                          l_cdnum AS cdnum,
                                          orchestrate_penamt AS penamt,
                                          orchestrate_hold AS hold,
                                          TO_DATE (orchestrate_issdt, 'yyyyddd') AS issdt,
                                          TO_DATE (orchestrate_matdt, 'yyyyddd') AS matdt,
                                          trim(orchestrate_acname) acname,
                                          l_status AS  status,
                                          TRIM (orchestrate_type) product_type,
                                          TRIM (orchestrate_renew) renew,
                                          orchestrate_dactn AS dactn,
                                          orchestrate_rate AS rate,
                                          orchestrate_wdrwh AS wdrwh
                                   FROM   (SELECT
                                          l_acc_no AS acctno,
                                          (case when length(orchestrate_brn) <= 2 then LPAD (orchestrate_brn, 3, '0') else TO_CHAR(orchestrate_brn) end) AS brn,
                                          orchestrate_cifno AS cifno,
                                          orchestrate_cdterm AS cdterm,
                                          trim(orchestrate_curtyp) AS curtyp,
                                          orchestrate_cbal AS cbal,
                                          orchestrate_orgbal AS orgbal,
                                          orchestrate_accint AS accint,
                                          l_cdnum AS cdnum,
                                          orchestrate_penamt AS penamt,
                                          orchestrate_hold AS hold,
                                          TO_DATE (orchestrate_issdt, 'yyyyddd') AS issdt,
                                          TO_DATE (orchestrate_matdt, 'yyyyddd') AS matdt,
                                          trim(orchestrate_acname) acname,
                                          l_status AS  status,
                                          TRIM (orchestrate_type) product_type,
                                          TRIM (orchestrate_renew) renew,
                                          orchestrate_dactn AS dactn,
                                          orchestrate_rate AS rate,
                                          orchestrate_wdrwh AS wdrwh
                                   FROM   DUAL)a, sync_cdc_cdmemo b
                                  WHERE  orchestrate_acctno <> 0
                                          AND TRIM (orchestrate_status) IS NOT NULL
                                           AND orchestrate_status <> 2 AND orchestrate_status <> 0
                                           AND a.acctno = b.acctno(+)
                                ) a
                           ON   (a.acctno = c.acct_no)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.bank_no = c_bank_no,
                           c.org_no = a.brn,
                           c.cif_no = a.cifno,
                           c.acct_type = 'FD',
                           c.currency_code = a.curtyp,
                           c.original_balance = a.orgbal,
                           c.principal_balance = a.cbal,
                           c.accured_interest = a.accint,
                           c.p_acct_no = a.cdnum,
                           c.penalty_amount = a.penamt,
                           c.current_cash_value =
                               (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
                           c.hold_amount = a.hold,
                           c.issued_date = a.issdt,
                           c.maturity_date = a.matdt,
                           c.acct_name = a.acname,
                           c.product_type = a.product_type,
                           c.interest_rate = a.rate,
                           c.status = a.status
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.acct_no,
                                            c.bank_no,
                                            c.org_no,
                                            c.branch_no,
                                            c.cif_no,
                                            c.acct_type,
                                            c.currency_code,
                                            c.original_balance,
                                            c.principal_balance,
                                            c.accured_interest,
                                            c.p_acct_no,
                                            c.penalty_amount,
                                            c.hold_amount,
                                            c.issued_date,
                                            c.maturity_date,
                                            c.acct_name,
                                            c.product_type,
                                            c.interest_rate,
                                            c.status,
                                            c.current_cash_value)
                           VALUES   (l_acc_no,
                                     c_bank_no,
                                     a.brn,
                                     a.brn,
                                     a.cifno,
                                     'FD',
                                     a.curtyp,
                                     a.orgbal,
                                     a.cbal,
                                     a.accint,
                                     a.cdnum,
                                     a.penamt,
                                     a.hold,
                                     a.issdt,
                                     a.matdt,
                                     a.acname,
                                     a.product_type,
                                     a.rate,
                                     a.status,
                                     (  a.cbal
                                      + a.accint
                                      - a.penamt
                                      - a.hold
                                      - a.wdrwh));

        l_taget_table := 'BC_RELATED_ACCOUNT';
                 --COMMIT ;--HAONS ADD 20150329
--                   MERGE INTO bc_related_account c
--                        USING   (SELECT
--                                       l_acc_no AS acctno,
--                                         l_status AS status,
--                                          b.user_id
--                                   FROM  bc_user_info b
--                                  WHERE TO_CHAR (TRIM(orchestrate_cifno)) = b.cif_no
--                                          AND b.status = 'ACTV'
--                                          AND orchestrate_acctno <> 0
--                                          AND TRIM (orchestrate_status) IS NOT NULL
--                                ) src
--                           ON (src.acctno = c.acct_no
--                                 AND src.user_id = c.user_id)
--                   WHEN MATCHED
--                   THEN
--                       UPDATE SET c.status = src.status
--                   WHEN NOT MATCHED
--                   THEN
--                       INSERT(c.relation_id,
--                                            c.user_id,
--                                            c.acct_no,
--                                            c.acct_type,
--                                            c.is_master,
--                                            c.status,
--                                            c.create_time)
--                           VALUES(seq_relation_id.NEXTVAL,
--                                     src.user_id,
--                                     src.acctno,
--                                     'FD',
--                                     'N',
--                                     src.status,
--                                     SYSDATE);
          --COMMIT ;--HAONS ADD 20150329;
            --    IF orchestrate_cdmuid LIKE '%EBANK%' THEN
                    SELECT CASE WHEN EXISTS(SELECT 1
                                                FROM sync_cdc_cdmemo cd
                                                WHERE cd.acctno = orchestrate_acctno)
                                THEN 1
                                ELSE 0
                            END
                    INTO l_cbal_check FROM dual ;
                    IF l_cbal_check > 0
                    THEN
                        MERGE INTO bk_account_info c
                         USING (SELECT orchestrate_acctno AS acctno,
                                       orchestrate_accint AS accint,
                                       orchestrate_penamt AS penamt,
                                       orchestrate_hold AS hold,
                                       CASE
                                           WHEN b.cbal > orchestrate_cbal THEN b.cbal
                                           ELSE orchestrate_cbal
                                       END
                                           AS cbal,
                                       orchestrate_wdrwh AS wdrwh
                                  FROM sync_cdc_cdmemo b
                                 WHERE b.acctno = orchestrate_acctno) a
                            ON (a.acctno = c.acct_no)
                        WHEN MATCHED
                        THEN
                            UPDATE SET
                                c.principal_balance = a.cbal,
                                c.current_cash_value =
                                    (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh);
                    ---DuongDV them ngay 28-07-2015:begin xu ly truong hop tiet kiem truc tuyen khong tat toan ngay duoc
                         MERGE INTO bk_receipt_info c
                         USING (SELECT (case when length(acctno) = 13 then LPAD (acctno, 14, '0') else TO_CHAR(acctno) end) AS acctno,
                                       orchestrate_accint AS accint,
                                       orchestrate_penamt AS penamt,
                                       CASE
                                           WHEN b.cbal > orchestrate_cbal THEN b.cbal
                                           ELSE orchestrate_cbal
                                       END
                                           AS cbal,
                                       fn_get_account_status_code(orchestrate_status) as status
                                  FROM sync_cdc_cdmemo b
                                  WHERE b.acctno = orchestrate_acctno) a
                            ON (a.acctno = c.receipt_no)
                          WHEN MATCHED
                          THEN
                            UPDATE
                            SET
                                 c.principal = a.cbal,
                                 c.interest_amount = a.accint,
                                 c.status = a.status
                            ;
                      -- DuongDV ngay 28-07-2015: eng
                    END IF;
               -- END IF;
            END IF;
        END IF;
        COMMIT ;


    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CDTNEW', l_taget_table,
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;


    END;

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created
 **  DuongDV    05/12/2014    Edit: them phan dong bo toi SYN_LNMEMO_CDC
----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_lnmemo_sync (dm_operation_type in CHAR,
                            orchestrate_accint IN NUMBER,
                            orchestrate_curtyp IN VARCHAR,
                            orchestrate_cbal IN NUMBER,
                            orchestrate_bilprn IN NUMBER ,
                            orchestrate_bilint IN NUMBER,
                            orchestrate_billc IN NUMBER,
                            orchestrate_bilesc IN NUMBER ,
                            orchestrate_biloc IN NUMBER,
                            orchestrate_bilmc IN NUMBER,
                            orchestrate_drlimt IN NUMBER,
                            orchestrate_hold IN NUMBER,
                            orchestrate_comacc IN NUMBER,
                            orchestrate_othchg IN NUMBER,
                            orchestrate_acctno IN NUMBER)
    IS
    l_err_desc VARCHAR2(250);
  BEGIN
    if dm_operation_type <> 'D' AND orchestrate_acctno > 0 then
          UPDATE  bk_account_info c
             SET c.accured_interest = orchestrate_accint,
                 c.currency_code = TRIM(orchestrate_curtyp),
                 c.os_principal = orchestrate_cbal,
                 c.billed_total_amount = orchestrate_bilprn + orchestrate_bilint + orchestrate_billc +
                                         orchestrate_bilesc + orchestrate_biloc + orchestrate_bilmc,
                 c.billed_principal = orchestrate_bilprn,
                 c.billed_interest = orchestrate_bilint,
                 c.billed_late_charge = orchestrate_billc,
                 c.overdraft_limit = orchestrate_drlimt,
                 c.hold_amount = orchestrate_hold,
                 c.accrued_common_fee  = orchestrate_comacc,
                 c.other_charges = orchestrate_othchg
                 where c.acct_no = (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end)
                 --AND
                 ;

            COMMIT;
        END IF;


    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'LNMEMO', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;

  END;

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created
 **  HaoNS      08/05/2015    --#20150508 HaoNS add for xu ly delete tai khoan LN

----------------------------------------------------------------------------------------------------*/
   PROCEDURE pr_lntnew_sync (dm_operation_type in CHAR,
                            orchestrate_brn in NUMBER,
                            orchestrate_accint in NUMBER,
                            orchestrate_cifno in NUMBER,
                            orchestrate_lnnum in NUMBER,
                            orchestrate_acctno in NUMBER,
                            orchestrate_purcod in VARCHAR,
                            orchestrate_curtyp in VARCHAR,
                            orchestrate_orgamt in NUMBER,
                            orchestrate_cbal in NUMBER,
                            orchestrate_ysobal in NUMBER,
                            orchestrate_billco in NUMBER,
                            orchestrate_freq in NUMBER,
                            orchestrate_ipfreq in NUMBER,
                            orchestrate_fulldt in NUMBER,
                            orchestrate_status in NUMBER,
                            orchestrate_odind in VARCHAR,
                            orchestrate_bilesc in NUMBER,
                            orchestrate_biloc in NUMBER,
                            orchestrate_bilmc in NUMBER,
                            orchestrate_bilprn in NUMBER,
                            orchestrate_bilint in NUMBER,
                            orchestrate_billc in NUMBER,
                            orchestrate_pmtamt in NUMBER,
                            orchestrate_fnlpmt in NUMBER,
                            orchestrate_drlimt in NUMBER,
                            orchestrate_hold in NUMBER,
                            orchestrate_accmlc in VARCHAR,
                            orchestrate_comacc in NUMBER,
                            orchestrate_othchg in NUMBER,
                            orchestrate_acname in VARCHAR,
                            orchestrate_type in VARCHAR,
                            orchestrate_datopn in NUMBER,
                            orchestrate_matdt in NUMBER,
                            orchestrate_freldt in VARCHAR,
                            orchestrate_rate in VARCHAR,
                            orchestrate_term in NUMBER,
                            orchestrate_tmcode in VARCHAR,
                            orchestrate_before_acctno in NUMBER,
                            orchestrate_before_cifno in NUMBER)
    IS
        l_check NUMBER(3);
        l_status VARCHAR2(10);
        l_acc_no VARCHAR2(14);
        l_err_desc VARCHAR2(250);

    BEGIN
        /*
        --#20150701 QuanPD add tam thoi: loai bo san pham MDB
        SELECT (CASE
                    WHEN EXISTS (   SELECT 1
                                    FROM ibs.cstb_branch_mdb_loai_tk
                                    WHERE branch_code = orchestrate_brn )
                    THEN 1
                    ELSE 0
                END)
        INTO l_check
        FROM DUAL;

        IF l_check > 0 THEN
            RETURN;
        END IF;
        --#20150701 QuanPD end
        */

        --#20141202 Loctx add for benifit info
        IF dm_operation_type <> 'D' THEN
            MERGE INTO cstb_account_info c
            USING (SELECT orchestrate_cifno cif_no,
                        orchestrate_acctno acct_no,
                        orchestrate_acname ac_name
                    FROM DUAL
                    ) A
            ON (c.acct_no =  a.acct_no AND c.module = 'LN')
            WHEN MATCHED THEN
                UPDATE SET c.acct_name = a.ac_name, c.cif_no = a.cif_no
            WHEN NOT MATCHED THEN
                INSERT  (module, cif_no, acct_no, acct_name)
                    VALUES ( 'LN', a.cif_no, a.acct_no, a.ac_name);

            COMMIT;
        ELSE
            --#20150508 HaoNS add for xu ly delete tai khoan LN
            DELETE  FROM bk_account_info a
                            WHERE a.acct_type = 'LN'
                            AND a.acct_no = (case when length(orchestrate_before_acctno) = 13 then LPAD (orchestrate_before_acctno, 14, '0') else TO_CHAR(orchestrate_before_acctno) end)
                            AND a.cif_no = orchestrate_before_cifno
                            AND extract(hour from cast(sysdate as timestamp)) > 6
                            AND extract(hour from cast(sysdate as timestamp)) < 18;
            COMMIT;
        END IF;


        IF dm_operation_type <> 'D' AND  orchestrate_acctno <> 0
                               AND TRIM (orchestrate_status) IS NOT NULL
                               AND orchestrate_status <> 2
        THEN
            l_status := fn_get_account_status_code (orchestrate_status);
            l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);
            SELECT CASE
                       WHEN EXISTS
                                (SELECT 1
                                   FROM (SELECT cif_no FROM bb_corp_info) a
                                  WHERE a.cif_no = TO_CHAR (orchestrate_cifno))
                       THEN 1
                       ELSE 0
                   END
              INTO l_check
              FROM DUAL;

            IF l_check = 1
            THEN
                MERGE INTO bk_account_info c
                 USING (SELECT (case when length(orchestrate_brn) <= 2 then LPAD (orchestrate_brn, 3, '0') else TO_CHAR(orchestrate_brn) end) AS brn,
                               orchestrate_accint AS accint,
                               orchestrate_type AS TYPE,
                               orchestrate_cifno AS cifno,
                               TRIM (orchestrate_lnnum) AS lnnum,
                               l_acc_no AS acctno,
                               orchestrate_purcod AS purcod,
                               orchestrate_curtyp AS curtyp,
                               orchestrate_orgamt AS orgamt,
                               orchestrate_cbal AS cbal,
                               orchestrate_ysobal AS ysobal,
                               orchestrate_billco AS billco,
                               orchestrate_term AS term,
                               orchestrate_freq AS freq,
                               orchestrate_ipfreq AS ipfreq,
                               DECODE (orchestrate_fulldt,
                                       0,NULL,TO_DATE(orchestrate_fulldt,'YYYYDDD'))
                                   AS fulldt,
                               l_status AS status,
                               orchestrate_odind AS odind,
                               orchestrate_bilprn AS bilprn,
                               orchestrate_bilint AS bilint,
                               orchestrate_billc AS billc,
                               orchestrate_bilesc AS bilesc,
                               orchestrate_biloc AS biloc,
                               orchestrate_bilmc AS bilmc,
                               orchestrate_pmtamt AS pmtamt,
                               orchestrate_fnlpmt AS fnlpmt,
                               orchestrate_drlimt AS drlimt,
                               orchestrate_hold AS hold,
                               orchestrate_accmlc AS accmlc,
                               orchestrate_comacc AS comacc,
                               orchestrate_othchg AS othchg,
                               TRIM (orchestrate_acname) acname,
                               TRIM (orchestrate_type) product_type,
                               DECODE(orchestrate_matdt,0,null,TO_DATE(orchestrate_matdt,'YYYYDDD')) AS matdt,--haons20142412
                               DECODE (orchestrate_datopn,
                                       0,null,
                                       TO_DATE(orchestrate_datopn,'YYYYDDD'))
                                   AS datopn,
                               DECODE (orchestrate_freldt,
                                       0,NULL,
                                       TO_DATE(orchestrate_freldt,'YYYYDDD'))
                                   AS freldt,
                               orchestrate_rate AS rate,
                               orchestrate_tmcode AS tmcode
                          FROM DUAL
                          ) a
                    ON (a.acctno = c.acct_no)
                WHEN MATCHED
                THEN
                    UPDATE SET c.status = a.status,
                                c.cif_no = a.cifno,
                                c.remark =  a.term || a.tmcode,
                                c.acct_name = a.acname
                WHEN NOT MATCHED
                THEN
                    INSERT (c.bank_no,
                            c.org_no,
                            c.branch_no,
                            c.accured_interest,           /*c.full_release_date,*/
                            c.cif_no,
                            c.loan_no,
                            c.acct_no,
                            c.purpose_code,
                            c.currency_code,
                            c.original_balance,
                            c.os_principal,
                            c.os_balance,                         /*c.loan_term,*/
                            c.principal_frequent,
                            c.interest_frequent,
                            c.full_release_date,
                            c.status,
                            c.overdue_indicator_description,
                            c.billed_total_amount,
                            c.billed_principal,
                            c.billed_interest,
                            c.billed_late_charge,
                            c.payment_amount,
                            c.final_payment_amount,
                            c.overdraft_limit,
                            c.hold_amount,
                            c.accrued_late_charge,
                            c.accrued_common_fee,
                            c.other_charges,
                            c.acct_type,
                            c.acct_name,
                            c.product_type,
                            c.issued_date,
                            c.maturity_date,
                            c.available_date,
                            c.interest_rate,
                            c.remark)
                    VALUES (
                               c_bank_no,
                               a.brn,
                               a.brn,
                               a.accint,
                               a.cifno,
                               a.lnnum,
                               a.acctno,
                               TRIM (a.purcod),
                               TRIM (a.curtyp),
                               a.orgamt,
                               a.cbal,
                               a.ysobal + a.billco,
                               TRIM (a.freq),
                               a.ipfreq,
                               a.fulldt,
                               a.status,
                               TRIM (a.odind),
                                 a.bilprn
                               + a.bilint
                               + a.billc
                               + a.bilesc
                               + a.biloc
                               + a.bilmc,
                               a.bilprn,
                               a.bilint,
                               a.billc,
                               a.pmtamt,
                               a.fnlpmt,
                               a.drlimt,
                               a.hold,
                               TRIM (a.accmlc),
                               a.comacc,
                               a.othchg,
                               'LN',
                               TRIM (a.acname),
                               TRIM (a.product_type),
                               a.datopn,
                               a.matdt,
                               a.freldt,
                               a.rate,
                               a.term || a.tmcode);

                COMMIT;
            END IF;
        END IF;


    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'LNTNEW', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc||'FULLDT='||orchestrate_fulldt||'MATDT='||orchestrate_matdt||'DATOPN='||orchestrate_datopn||'FRELDT='||orchestrate_freldt);
            COMMIT;
            RAISE;

    END;

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **     Xu ly truong hop tai khoan LN va xoa ngay trong ngay, sau do so tai khoan LN nay lai duoc dung cho nguoi khac
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/03/2015   Created:

----------------------------------------------------------------------------------------------------*/
/*
    PROCEDURE pr_lntnew_del_sync (dm_operation_type in CHAR,
                            orchestrate_cifno in NUMBER,
                            orchestrate_acctno in NUMBER)
    IS
        l_err_desc VARCHAR2(250);
        l_row_id VARCHAR2(100);
     --   l_acc_num VARCHAR2(20);
        CURSOR del_acc_lntnew IS SELECT ROWID--, a.acct_no
                        FROM bk_account_info a
                            WHERE a.acct_type = 'LN'
                            AND a.acct_no = LPAD(orchestrate_acctno,14,'0')
                            AND a.cif_no = orchestrate_cifno
                            AND extract(hour from cast(sysdate as timestamp)) > 6
                            AND extract(hour from cast(sysdate as timestamp)) < 18;
    BEGIN
        IF dm_operation_type = 'D' THEN
           OPEN del_acc_lntnew;
            LOOP
            FETCH del_acc_lntnew
                INTO l_row_id;--,
                  --  l_acc_num
                   -- ;
                    EXIT WHEN del_acc_lntnew%NOTFOUND;
                    DELETE FROM bk_account_info
                        WHERE ROWID = l_row_id;
                        --AND acct_no = l_acc_num
                      --  ;
                  cspks_cdc_util.pr_log_sync_error('SIBS', 'LNTNEW_DELETE', 'LNTNEW_DELETE',
                        orchestrate_acctno, dm_operation_type,
                        'orchestrate_cifno= '|| orchestrate_cifno||'_'||sysdate
                        );
            END LOOP;
            CLOSE del_acc_lntnew;
            COMMIT;
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            CLOSE del_acc_lntnew;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'LNTNEW_DELETE', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
*/
 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/
--------------------
/*
PROCEDURE pr_cdgroup_sync_b(dm_operation_type IN CHAR,
                                orchestrate_cfgnam IN VARCHAR,
                                orchestrate_cfgcur IN VARCHAR,
                                orchestrate_cfagd7 IN NUMBER,
                                orchestrate_cfgsts IN CHAR,
                                orchestrate_cfagno IN NUMBER,
                                orchestrate_cfcifn IN NUMBER)
     IS
      l_check NUMBER := 0;
      l_err_desc VARCHAR2(250);
      BEGIN

        IF dm_operation_type IN ('U','I') AND trim(orchestrate_cfgcur) IS NOT NULL
        THEN
            SELECT CASE WHEN EXISTS(SELECT 1 FROM (SELECT cif_no
                                                    FROM bc_user_info
                                                  UNION
                                                  SELECT cif_no FROM bb_corp_info)a
                                    WHERE a.cif_no =TRIM (orchestrate_cfcifn))
                        THEN 1
                        ELSE 0
                    END
            INTO l_check
            FROM dual;

            IF l_check =1 AND dm_operation_type ='I' THEN
                      INSERT INTO   bk_account_info c(c.acct_no,
                         c.acct_name,
                         c.cif_no,
                         c.acct_type,
                         c.currency_code,
                         c.org_no,
                         c.branch_no,
                         c.establish_date,
                         c.issued_date,
                         c.status,
                         c.update_time,
                         c.bank_no)
                        (SELECT src.cfagno,
                        trim(src.cfgnam),
                        src.cfcifn,
                        'FD',
                        TRIM (src.cfgcur),
                        SUBSTR (src.cfagno, 0, 3),
                        SUBSTR( src.cfagno, 0, 3),
                        src.cfagd7,
                        src.cfagd7,
                        src.cfgsts,
                        SYSDATE,
                        c_bank_no FROM ((SELECT   orchestrate_cfcifn AS cfcifn,
                                    LPAD (orchestrate_cfagno, 14, '0') cfagno,
                                    DECODE (TRIM (orchestrate_cfgsts),
                                            'N', 'ACTV',
                                            'C', 'ACTV',
                                            'CLOS') cfgsts,
                                    trim(orchestrate_cfgnam) cfgnam,
                                    orchestrate_cfgcur AS cfgcur,
                                    DECODE (LENGTH (orchestrate_cfagd7), 7,
                                        TO_DATE (orchestrate_cfagd7, 'yyyyddd'),
                                    NULL) cfagd7
                                FROM  dual                                    --WHERE trim(orchestrate_cfgcur) is not null
                             )
                             src));
             ELSE
                 UPDATE  bk_account_info c SET
                        (c.acct_name,
                        c.currency_code,
                        c.issued_date ,
                        c.status ,
                        c.bank_no ,
                        c.branch_no) =
                        ( SELECT trim(src.cfgnam),
                            trim(src.cfgnam),
                            TRIM (src.cfgcur),
                            src.cfagd7,
                            src.cfgsts,
                            c_bank_no,
                            SUBSTR (src.cfagno, 0, 3)
                            FROM (SELECT   orchestrate_cfcifn AS cfcifn,
                                    LPAD (orchestrate_cfagno, 14, '0') cfagno,
                                    DECODE (TRIM (orchestrate_cfgsts),
                                            'N', 'ACTV',
                                            'C', 'ACTV',
                                            'CLOS') cfgsts,
                                    trim(orchestrate_cfgnam) cfgnam,
                                    orchestrate_cfgcur AS cfgcur,
                                    DECODE (LENGTH (orchestrate_cfagd7), 7,
                                        TO_DATE (orchestrate_cfagd7, 'yyyyddd'),
                                    NULL) cfagd7
                                FROM  dual--WHERE trim(orchestrate_cfgcur) is not null
                             )
                             src WHERE src.cfagno = c.acct_no
                                     AND src.cfcifn = c.cif_no);


            END IF;
        END IF;
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFAGRP', 'BK_ACCOUNT_INFO',
                                                    orchestrate_cfagno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;*/
-----------------------

     PROCEDURE pr_cdgroup_sync(dm_operation_type IN CHAR,
                                orchestrate_cfgnam IN VARCHAR,
                                orchestrate_cfgcur IN VARCHAR,
                                orchestrate_cfagd7 IN NUMBER,
                                orchestrate_cfgsts IN CHAR,
                                orchestrate_cfagno IN NUMBER,
                                orchestrate_cfcifn IN NUMBER)
     IS
      l_check NUMBER := 0;
      l_err_desc VARCHAR2(250);
      BEGIN

        IF dm_operation_type <> 'D' AND trim(orchestrate_cfgcur) IS NOT NULL
        THEN
            SELECT CASE WHEN EXISTS(SELECT 1 FROM (
                                                  SELECT cif_no FROM bb_corp_info)a
                                    WHERE a.cif_no =TRIM (orchestrate_cfcifn))
                        THEN 1
                        ELSE 0
                    END
            INTO l_check
            FROM dual;

            IF l_check =1 THEN
                MERGE INTO   bk_account_info c
                        USING   (SELECT   orchestrate_cfcifn AS cfcifn,
                                    (case when length(orchestrate_cfagno) = 13 then LPAD (orchestrate_cfagno, 14, '0') else TO_CHAR(orchestrate_cfagno) end) cfagno,
                                    DECODE (TRIM (orchestrate_cfgsts),
                                            'N', 'ACTV',
                                            'C', 'ACTV',
                                            'CLOS') cfgsts,
                                    trim(orchestrate_cfgnam) cfgnam,
                                    orchestrate_cfgcur AS cfgcur,
                                    DECODE (LENGTH (orchestrate_cfagd7), 7,
                                        TO_DATE (orchestrate_cfagd7, 'yyyyddd'),
                                    NULL) cfagd7
                                FROM  dual
                                    --WHERE trim(orchestrate_cfgcur) is not null
                             )
                             src
                        ON   (src.cfagno = c.acct_no
                        --AND src.cfcifn = c.cif_no-- #20150325 HAONS ADD --#20150402 LocTX remove
                        )
                WHEN MATCHED
                THEN
                    UPDATE SET
                        c.acct_name = trim(src.cfgnam),
                        c.currency_code = TRIM (src.cfgcur),
                        c.issued_date = src.cfagd7,
                        c.status = src.cfgsts,
                        c.bank_no = c_bank_no,
                        c.branch_no = SUBSTR (src.cfagno, 0, 3)
                WHEN NOT MATCHED
                THEN
                    INSERT(c.acct_no,
                         c.acct_name,
                         c.cif_no,
                         c.acct_type,
                         c.currency_code,
                         c.org_no,
                         c.branch_no,
                         c.establish_date,
                         c.issued_date,
                         c.status,
                         c.update_time,
                         c.bank_no)
                    VALUES   (src.cfagno,
                        trim(src.cfgnam),
                        src.cfcifn,
                        'FD',
                        TRIM (src.cfgcur),
                        SUBSTR (src.cfagno, 0, 3),
                        SUBSTR( src.cfagno, 0, 3),
                        src.cfagd7,
                        src.cfagd7,
                        src.cfgsts,
                        SYSDATE,
                        c_bank_no);

                COMMIT;
            END IF;
        END IF;


    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'CFAGRP', 'BK_ACCOUNT_INFO',
                                                    orchestrate_cfagno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
/*
BEGIN
    SELECT   *
    INTO   logrow
    FROM   tlogdebug
    WHERE   ROWNUM <= 1;

    pkgctx      :=
    plog.init('cspkg_account_sync',
        plevel => logrow.loglevel,
        plogtable => ( logrow.log4table = 'Y' ),
        palert => ( logrow.log4alert = 'Y' ),
        ptrace => ( logrow.log4trace = 'Y' ) );*/
END;

/
