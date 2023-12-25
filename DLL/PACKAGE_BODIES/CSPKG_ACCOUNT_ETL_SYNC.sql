--------------------------------------------------------
--  DDL for Package Body CSPKG_ACCOUNT_ETL_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_ACCOUNT_ETL_SYNC" 
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
 **  LocTX      31/10/2014    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/
    pkgctx    plog.log_ctx;
    logrow    tlogdebug%ROWTYPE;

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_lnmast_sync
    IS
        l_max_cif NUMBER;
        l_min_count NUMBER;
        l_max_count NUMBER;
        l_acct_cls ty_varchar2_tb;
        l_error_desc VARCHAR2(300);

    BEGIN
        plog.setbeginsection (pkgctx, 'pr_lnmast_sync');

        l_max_cif := 0;

        l_min_count   := 0;
        l_max_count   := 0;

        DBMS_STATS.gather_table_stats (
				ownname    => 'IBS',
				tabname    => 'sync_etl_lnmast'
		     	);

        SELECT MAX(a.cifno) + 1
        INTO   l_max_cif
        FROM   sync_etl_lnmast a;

        SELECT MIN(a.cifno)
        INTO   l_min_count
        FROM   sync_etl_lnmast a;

        IF (l_max_cif IS NULL)
        THEN
          l_max_cif := 0;
        END IF;

        IF (l_min_count IS NULL)
        THEN
          l_min_count := 0;
        END IF;

        ---l_max_cif := 100000;


        LOOP
            BEGIN
                l_max_count := l_min_count + g_limit_count;

--                plog.debug(pkgctx, 'l_min_count = ' || l_min_count || ', l_max_count = ' || l_max_count);
                dbms_output.put_line('l_min_count = ' || l_min_count || ', l_max_count = ' || l_max_count);

                MERGE INTO   bk_account_info c
                     USING   (SELECT b.bkn,
                                      (case when length(b.brn) <= 2 then LPAD (b.brn, 3, '0') else TO_CHAR(b.brn) end) brn,
                                       b.accint,              --LNPDUE.PDIINT,
                                       TRIM (b.type) product_type,
                                       b.cifno,
                                       b.lnnum,
                                       (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) as acctno,
                                       b.purcod,
                                       b.curtyp,
                                       b.orgamt,
                                       b.amtrel,
                                       b.cbal,
                                       b.ysobal,
                                       b.billco,
                                       b.term,
                                       b.freq,
                                       b.frcode,
                                       b.ipfreq,
                                       b.ipcode,
                                       DECODE (b.fulldt,
                                          0, NULL,
                                          TO_DATE (b.fulldt, 'yyyyddd'))
                                       fulldt,
                                       fn_get_account_status_code(b.status) status,
                                       b.odind,
                                       b.bilprn,
                                       b.bilint,
                                       b.billc,
                                       b.bilesc,
                                       b.biloc,
                                       b.bilmc,
                                       b.pmtamt,
                                       b.fnlpmt,
                                       b.drlimt,
                                       b.hold,
                                       b.accmlc,
                                       b.comacc,
                                       b.othchg,
                                       trim(b.acname) acname,
                                       DECODE (b.datopn,
                                              0, NULL,
                                              TO_DATE (b.datopn, 'yyyyddd')
                                       ) datopn,
                                       DECODE (b.matdt,
                                                0, NULL,
                                                TO_DATE (b.matdt, 'yyyyddd')
                                       ) matdt,
                                       DECODE (b.FRELDT, 0, NULL, TO_DATE (b.FRELDT, 'yyyyddd')) FRELDT,
                                       b.rate,
                                       b.tmcode
                                FROM   sync_etl_lnmast b/*,
                                (SELECT distinct cif_no--#20150121 Loctx add them so voi soiurce code goc
                                      FROM   bc_user_info
                                      UNION
                                      SELECT distinct cif_no
                                      FROM   bb_corp_info
                                      )cif*/--#20150409 Loctx disable vi loi cap nhat lai cif
                               WHERE   b.acctno <> 0
                                       AND b.status > 0 --#20150119 HaoNS change from b.status is not null
                                       --AND b.cifno BETWEEN l_min_count AND  l_max_count
                                       AND b.cifno >= l_min_count AND b.cifno < l_max_count--#20150119 Loctx change
									    -- 20210625, ChiPM loc cac tai khoan dong so huu o bang cdc_cfacct
                                        AND NOT EXISTS (select 1 from cdc_cfacct where cdc_cfacct.CFACCN = b.acctno)
                                       --AND cif.cif_no = TO_CHAR(b.cifno) --#20150121 Loctx add them so voi soiurce code goc
                                        /*
                                       -- 20150701 QuanPD add tam thoi: loai bo san pham MDB
                                       AND NOT EXISTS (SELECT * FROM ibs.cstb_branch_mdb_loai_tk mdb
                                                        WHERE LPAD (mdb.branch_code, 3, '0') = b.brn)
                                        */
                             ) a
                        ON   (a.acctno = c.acct_no
                              --AND a.update_time ...  --#20141101 Loctx add for check last update
                         )
                WHEN MATCHED
                THEN
                    UPDATE SET
                        c.bank_no = '302',                            --a.bkn,
                        c.org_no = a.brn,              --a.brn,
                        c.branch_no = a.brn,           --a.brn,
                        c.accured_interest = a.accint,
                        --c.full_release_date?
                        c.loan_no = TRIM (a.lnnum),
                        c.purpose_code = TRIM (a.purcod),
                        c.currency_code = TRIM (a.curtyp),
                        c.original_balance = a.orgamt,
                        c.os_principal = a.cbal,
                        c.os_balance = a.ysobal + a.billco,
                        c.remark           = a.TERM || a.tmcode,
                        c.principal_frequent = TRIM (a.freq),
                        c.interest_frequent = a.ipfreq,
                        c.full_release_date = fulldt,
                        c.status = a.status,
                        c.overdue_indicator_description = TRIM (a.odind),
                        c.billed_total_amount =
                              a.bilprn
                            + a.bilint
                            + a.billc
                            + a.bilesc
                            + a.biloc
                            + a.bilmc,
                        c.billed_principal = a.bilprn,
                        c.billed_interest = a.bilint,
                        c.billed_late_charge = a.billc,
                        c.payment_amount = a.pmtamt,
                        c.final_payment_amount = a.fnlpmt,
                        c.overdraft_limit = a.drlimt,
                        c.available_limit = a.orgamt - a.amtrel, --amtrel:so tien giai ngan, orgamt:so tien goc
                        c.hold_amount = a.hold,
                        c.accrued_late_charge = TRIM (a.accmlc),
                        c.accrued_common_fee = a.comacc,
                        c.other_charges = a.othchg,
                        c.acct_type = 'LN',
                        c.acct_name = TRIM (a.acname),
                        c.product_type = TRIM (a.product_type),
                        c.issued_date = a.datopn,
                        c.maturity_date = a.matdt,
                        c.cif_no = a.cifno,
                        c.available_date = a.freldt,
                        c.interest_rate=  a.rate--,
                        --c.update_time = SYSDATE--#20141101 Loctx add for check last update
                WHEN NOT MATCHED
                THEN
                    INSERT              (c.bank_no,
                                         c.org_no,
                                         c.branch_no,
                                         c.accured_interest, /*c.full_release_date,*/
                                         c.cif_no,
                                         c.loan_no,
                                         c.acct_no,
                                         c.purpose_code,
                                         c.currency_code,
                                         c.original_balance,
                                         c.os_principal,
                                         c.os_balance,        /*c.loan_term,*/
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
                                         c.available_limit,
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
                                         c.remark--,
                                         --c.update_time
                                         )
                        VALUES   ('302',                              --a.bkn,
                                  a.brn,               --a.brn,
                                  a.brn,               --a.brn,
                                  a.accint,
                                  a.cifno,
                                  TRIM (a.lnnum),
                                  a.acctno,
                                  TRIM (a.purcod),
                                  TRIM (a.curtyp),
                                  a.orgamt,
                                  a.cbal,
                                  a.ysobal + a.billco,             /*a.TERM,*/
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
                                  a.orgamt - a.amtrel, --amtrel:so tien giai ngan, orgamt:so tien goc
                                  a.hold,
                                  TRIM (a.accmlc),
                                  a.comacc,
                                  a.othchg,
                                  'LN',
                                  TRIM (a.acname),
                                  TRIM (a.product_type),
                                  datopn,
                                  a.matdt,
                                  a.freldt,
                                  a.rate,
                                  a.TERM || a.tmcode
                              );

                COMMIT;
                l_min_count := l_max_count;

                --khong co them ban ghi nao
                EXIT WHEN (l_max_count > l_max_cif);
            END;
        END LOOP;


        SELECT acct_no BULK COLLECT
        INTO   l_acct_cls
        FROM   bk_account_info
        WHERE  status = 'CLOS'
        AND    acct_type = 'LN';

        --#20160427 Loctx add theo y/c cua Luong de tracking. update comment  b? bk_account_info_close 08/10/2018
       /* FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          INSERT INTO bk_account_info_close(
            acct_no, acct_name, p_acct_no, bank_no, org_no,
            product_type, loan_no, branch_no, sub_branch_no,
            cb_sign, cert_type, cert_code, acct_type,
            sub_acct_type, currency_code, cx_sign, establish_date,
            available_date, current_balance, freezed_balance,
            available_balance, remark, status, create_by,
            create_time, update_by, update_time, cif_no,
            ledger_balance, hold_amount, overdraft_limit,
            interest_rate, date_account_opened, passbook_no,
            accured_interest, term, original_balance,
            principal_balance, penalty_amount, current_cash_value,
            earmarked_amount, issued_date, maturity_date,
            times_nenewed_count, purpose_code, loan_original_amt,
            os_principal, os_balance, loan_term, principal_frequent,
            interest_frequent, full_release_date,
            overdue_indicator_description, billed_total_amount,
            billed_principal, billed_interest, billed_late_charge,
            payment_amount, final_payment_amount, accrued_late_charge,
            accrued_common_fee, other_charges, available_limit,
            tc_code_loan, last_run_time_loan
          )
          SELECT acct_no, acct_name, p_acct_no, bank_no, org_no,
            product_type, loan_no, branch_no, sub_branch_no,
            cb_sign, cert_type, cert_code, acct_type,
            sub_acct_type, currency_code, cx_sign, establish_date,
            available_date, current_balance, freezed_balance,
            available_balance, remark, status, create_by,
            create_time, update_by, update_time, cif_no,
            ledger_balance, hold_amount, overdraft_limit,
            interest_rate, date_account_opened, passbook_no,
            accured_interest, term, original_balance,
            principal_balance, penalty_amount, current_cash_value,
            earmarked_amount, issued_date, maturity_date,
            times_nenewed_count, purpose_code, loan_original_amt,
            os_principal, os_balance, loan_term, principal_frequent,
            interest_frequent, full_release_date,
            overdue_indicator_description, billed_total_amount,
            billed_principal, billed_interest, billed_late_charge,
            payment_amount, final_payment_amount, accrued_late_charge,
            accrued_common_fee, other_charges, available_limit,
            tc_code_loan, last_run_time_loan
        FROM bk_account_info
        WHERE  acct_no = l_acct_cls(i); */


        FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          DELETE FROM bk_account_info
          WHERE  acct_no = l_acct_cls(i);

        COMMIT;

        l_acct_cls.delete;

--        SELECT acct_no BULK COLLECT
--        INTO   l_acct_cls
--        FROM   bc_related_account
--        WHERE  status = 'CLOS';

--        FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
--          DELETE FROM bc_related_account a
--          WHERE  acct_no = l_acct_cls(i);

		-- 25/06/2021: update de loc cac tai khoan FD GROUP dong so huu
--        update bk_account_info set status = 'DLTD' where acct_no in (select lpad(cfaccn, 14 , '0') from cdc_cfacct);commit; 

--        COMMIT;

        plog.setendsection( pkgctx, 'pr_lnmast_sync' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc || 'l_min_count = ' || l_min_count || ',l_max_count=' || l_max_count);
            plog.setendsection( pkgctx, 'pr_lnmast_sync' );
            --forward error
            RAISE;
    END;
 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  DuongDV      08/12/2014    Created

----------------------------------------------------------------------------------------------------*/
   PROCEDURE pr_lnmemo_cdc_override
    IS
    l_err_desc VARCHAR2(250);
  BEGIN

    MERGE INTO bk_account_info c
    USING
    (
        select
                accint as accured_interest,
                TRIM(curtyp) as currency_code,
                cbal as os_principal ,
                (bilprn + bilint + billc + bilesc + biloc + bilmc) as billed_total_amount,
                bilprn as billed_principal,
                bilint as billed_interest,
                billc as billed_late_charge,
                drlimt as overdraft_limit,
                hold as hold_amount,
                comacc as accrued_common_fee,
                othchg as other_charges,
                (case when length(acctno) = 13 then LPAD (acctno, 14, '0') else TO_CHAR(acctno) end) as acct_no
                from RAWSTAGE.si_dat_lnmemo@RAWSTAGE_PRO_CORE

    )src
    ON (src.acct_no = c.acct_no)
    WHEN MATCHED THEN
    UPDATE
    SET
                c.accured_interest = src.accured_interest,
                 c.currency_code = src.currency_code,
                 c.os_principal = src.os_principal,
                 c.billed_total_amount = src.billed_total_amount,
                 c.billed_principal = src.billed_principal,
                 c.billed_interest = src.billed_interest,
                 c.billed_late_charge = src.billed_late_charge,
                 c.overdraft_limit = src.overdraft_limit,
                 c.hold_amount = src.hold_amount,
                 c.accrued_common_fee  = src.accrued_common_fee,
                 c.other_charges = src.other_charges

    ;

    COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            --cspks_cdc_util.pr_log_sync_error('SIBS', 'LNMEMO', 'BK_ACCOUNT_INFO',
            --                                        orchestrate_acctno, dm_operation_type, l_err_desc);
           -- COMMIT;
           -- RAISE;

  END;
 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_ddmast_sync IS

        l_max_cif NUMBER;
        l_min_count NUMBER;
        l_max_count NUMBER;
        l_acct_cls ty_varchar2_tb;

        l_error_desc VARCHAR2(300);

    BEGIN
        plog.setbeginsection( pkgctx, 'pr_ddmast_sync' );

        l_max_count   := 0;
        -- comment 16/10/2018
        DBMS_STATS.gather_table_stats (
				ownname    => 'IBS',
				tabname    => 'sync_etl_ddmast'
		     	);
        ---
        SELECT MAX(a.cifno) + 1
        INTO   l_max_cif
        FROM   sync_etl_ddmast a;

        SELECT MIN(a.cifno)
        INTO   l_min_count
        FROM   sync_etl_ddmast a;

        IF (l_max_cif IS NULL)
        THEN
          l_max_cif := 0;
        END IF;

        IF (l_min_count IS NULL)
        THEN
          l_min_count := 0;
        END IF;


        LOOP
                   l_max_count := l_min_count + g_limit_count;

                   MERGE INTO   bk_account_info c
                        USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
                                       a .bankno,
                                       (case when length(a.branch) <= 2 then LPAD (a.branch, 3, '0') else TO_CHAR(a.branch) end) branch,
                                         (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end) acctno,
                                          fn_get_actype_code(SUBSTR(TRIM(a.actype),1,1)) actype,
                                          DECODE(LENGTH(TRIM (a.ddctyp)),3,TRIM (a.ddctyp),SUBSTR(TRIM (a.ddctyp),0,3))  ddctyp,
                                          a.cifno,
                                          fn_get_account_status_code(a.status) status,
                                          DECODE(a.datop7,0,null,TO_DATE (a.datop7, 'yyyyddd')) datop7,
                                          a.hold,
                                          a.cbal,
                                          a.odlimt,
                                          a.whhirt,
                                         TRIM (a.acname) acname,
                                          TRIM (a.sccode) product_type,
                                          a.rate,
                                          a.accrue
                                   FROM   sync_etl_ddmast a/*,
                                    (SELECT distinct cif_no--#20150121 Loctx add them so voi soiurce code goc
                                      FROM   bc_user_info
                                      UNION
                                      SELECT distinct cif_no
                                      FROM   bb_corp_info
                                      )cif*/--#20150409 Loctx disable vi loi cap nhat lai cif
                                  WHERE  a.acctno <> 0
                                          AND a.status >0
                                          AND  
                                          ( 
                                             SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '31'
                                             OR
                                             (    SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) = '31'
                                                  AND   
                                                  (TRIM (a.sccode)  in   ('R-CATCSTK','C-CATCSTK','R-CATCONL','R-CATCMLCO')
                                                  --TRIM (a.sccode)  in   ('R-CATCSTK','C-CATCSTK','R-CATCONL','R-CATCMLCO','L-CA-FDI','S-CA-FDI','S-SA11-FDI','L-CA FPI', 'S-CA FPI','A-CALOANNN','B-CAOFDI','B-CA-FDI','B-CA-KFPI','B-CA-OFPI','B-CAOFDI','B-CALOANNN','B-CAOTFDI','B-EscrVL1','CA11ODS1','CA11ODS0')
                                                    or LPAD(a.acctno, 14, '0') in (select acct_no from BB_RELATED_ACCOUNT_31 where status='ACTV')
                                                    )
                                              )
                                           ) 
                                          AND SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '13'
                                          AND a.cifno >= l_min_count AND a.cifno < l_max_count
										  -- 20210625, ChiPM loc cac tai khoan dong so huu o bang cdc_cfacct
                                         AND NOT EXISTS (select 1 from cdc_cfacct where cdc_cfacct.CFACCN = a.acctno)
                                          ---AND cif.cif_no = TO_CHAR(a.cifno) --#20150121 Loctx add them so voi soiurce code goc
                                            /*

                                          -- 20150701 QuanPD add tam thoi: loai bo san pham MDB
                                          AND NOT EXISTS (SELECT * FROM ibs.cstb_branch_mdb_loai_tk mdb
                                                            WHERE mdb.branch_code = a.branch)
                                            */
                                ) src
                           ON   (src.acctno = c.acct_no)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.org_no = src.branch,
                           c.branch_no = src.branch,
                           c.currency_code = src.ddctyp,
                           c.bank_no = '302',       --a.bankno, core: 27, ibs:302
                           c.status = DECODE(c.status, 'VIEW', c.status, src.status),
                           c.hold_amount = src.hold,
                           c.ledger_balance = src.cbal,
                           c.available_balance = src.cbal - src.hold + src.odlimt,
                           c.overdraft_limit = src.odlimt,
                           c.acct_name = trim(src.acname),
                           c.product_type = DECODE(c.product_type, 'KQ', c.product_type, src.product_type),
                           c.acct_type = DECODE(c.acct_type, '31', c.acct_type, src.actype), --s:sa, d:ca
                           c.issued_date = src.datop7,
                           c.cif_no = src.cifno,
                           c.interest_rate = src.rate,
                           c.accured_interest = src.accrue
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.acct_no,
                                            c.acct_name,
                                            c.p_acct_no,
                                            c.bank_no,
                                            c.org_no,
                                            c.product_type,
                                            c.branch_no,
                                            c.sub_branch_no,
                                            c.acct_type,
                                            c.sub_acct_type,
                                            c.currency_code,
                                            c.available_balance,
                                            c.status,
                                            c.create_time,
                                            c.update_time,
                                            c.cif_no,
                                            c.ledger_balance,
                                            c.hold_amount,
                                            c.date_account_opened,
                                            c.overdraft_limit,
                                            c.issued_date,
                                            c.interest_rate,
                                            c.accured_interest)
                           VALUES   (src.acctno,
                                     trim(src.acname),
                                     NULL,
                                     '302',
                                     src.branch,
                                     src.product_type,
                                     src.branch,
                                     NULL,
                                   src.actype,
                                     NULL,
                                     src.ddctyp,
                                     (src.cbal - src.hold + src.odlimt),
                                     src.status,
                                     SYSDATE,
                                     SYSDATE,
                                     src.cifno,
                                     src.cbal,
                                     src.hold,
                                     NULL,
                                     src.odlimt,
                                    src.datop7,
                                     src.rate,
                                     src.accrue);

                   COMMIT;


                   l_min_count := l_max_count;

                   --khong co them ban ghi nao
                   EXIT WHEN (l_max_count > l_max_cif);
        END LOOP;

        SELECT bai.acct_no BULK COLLECT
        INTO   l_acct_cls
        FROM   bk_account_info bai
        WHERE  bai.status = 'CLOS'
        AND    (acct_type = 'SA' OR acct_type = 'CA');

        --#20160427 Loctx add theo y/c cua Luong de tracking update comment  b? bk_account_info_close 08/10/2018
      /*  FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          INSERT INTO bk_account_info_close(
            acct_no, acct_name, p_acct_no, bank_no, org_no,
            product_type, loan_no, branch_no, sub_branch_no,
            cb_sign, cert_type, cert_code, acct_type,
            sub_acct_type, currency_code, cx_sign, establish_date,
            available_date, current_balance, freezed_balance,
            available_balance, remark, status, create_by,
            create_time, update_by, update_time, cif_no,
            ledger_balance, hold_amount, overdraft_limit,
            interest_rate, date_account_opened, passbook_no,
            accured_interest, term, original_balance,
            principal_balance, penalty_amount, current_cash_value,
            earmarked_amount, issued_date, maturity_date,
            times_nenewed_count, purpose_code, loan_original_amt,
            os_principal, os_balance, loan_term, principal_frequent,
            interest_frequent, full_release_date,
            overdue_indicator_description, billed_total_amount,
            billed_principal, billed_interest, billed_late_charge,
            payment_amount, final_payment_amount, accrued_late_charge,
            accrued_common_fee, other_charges, available_limit,
            tc_code_loan, last_run_time_loan
          )
          SELECT acct_no, acct_name, p_acct_no, bank_no, org_no,
            product_type, loan_no, branch_no, sub_branch_no,
            cb_sign, cert_type, cert_code, acct_type,
            sub_acct_type, currency_code, cx_sign, establish_date,
            available_date, current_balance, freezed_balance,
            available_balance, remark, status, create_by,
            create_time, update_by, update_time, cif_no,
            ledger_balance, hold_amount, overdraft_limit,
            interest_rate, date_account_opened, passbook_no,
            accured_interest, term, original_balance,
            principal_balance, penalty_amount, current_cash_value,
            earmarked_amount, issued_date, maturity_date,
            times_nenewed_count, purpose_code, loan_original_amt,
            os_principal, os_balance, loan_term, principal_frequent,
            interest_frequent, full_release_date,
            overdue_indicator_description, billed_total_amount,
            billed_principal, billed_interest, billed_late_charge,
            payment_amount, final_payment_amount, accrued_late_charge,
            accrued_common_fee, other_charges, available_limit,
            tc_code_loan, last_run_time_loan
        FROM bk_account_info
        WHERE  acct_no = l_acct_cls(i);*/

        FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          DELETE FROM bk_account_info
          WHERE  acct_no = l_acct_cls(i);

        COMMIT;

        l_acct_cls.delete;

--        SELECT bra.acct_no BULK COLLECT
--        INTO   l_acct_cls
--        FROM   bc_related_account bra
--        WHERE  bra.status = 'CLOS';

--        FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
--          DELETE FROM bc_related_account a
--          WHERE  a.acct_no = l_acct_cls(i);

		-- 25/06/2021: update de loc cac tai khoan FD GROUP dong so huu
--        update bk_account_info set status = 'DLTD' where acct_no in (select lpad(cfaccn, 14 , '0') from cdc_cfacct);commit; 

--        COMMIT;
        plog.setendsection( pkgctx, 'pr_ddmast_sync' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc || 'l_min_count = ' || l_min_count || ',l_max_count=' || l_max_count);
            plog.setendsection( pkgctx, 'pr_ddmast_sync' );
            --forward error
            RAISE;
    END;
 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  DUONGDV      08/12/2014    Created

----------------------------------------------------------------------------------------------------*/
   PROCEDURE pr_ddmemo_cdc_override
    IS
        l_err_desc VARCHAR2(250);

    BEGIN

        MERGE INTO bk_account_info c
        USING
        (
            select
                    fn_get_account_status_code (status) as status,
                    hold as hold_amount,
                    cbal as ledger_balance,
                    (cbal
                           - hold
                           + odlimt) as available_balance,
                    odlimt as overdraft_limit,
                    TRIM (acname) as acct_name,
                    (case when length(acctno) = 13 then LPAD (acctno, 14, '0') else TO_CHAR(acctno) end) as acct_no

            from RAWSTAGE.SI_DAT_DDMEMO_N@RAWSTAGE_PRO_CORE --sync_cdc_ddmemo
            where TRIM (status) IS NOT NULL

        )src
        ON(src.acct_no = c.acct_no)
        WHEN MATCHED THEN
        UPDATE
        SET
                       c.status = src.status,
                       c.hold_amount = src.hold_amount,
                       c.ledger_balance = src.ledger_balance,
                       c.available_balance = src.available_balance,
                       c.overdraft_limit = src.overdraft_limit,
                       c.acct_name = src.acct_name
        ;
        COMMIT;


    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

  END;
 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_cdmast_sync IS
        l_max_cif NUMBER;
        l_min_count NUMBER;
        l_max_count NUMBER;
        l_acct_cls ty_varchar2_tb;
        l_error_desc VARCHAR2(300);

    BEGIN
        plog.setbeginsection( pkgctx, 'pr_cdmast_sync' );

        l_min_count   := 0;
        l_max_count   := 0;

         -- comment 16/10/2018 bc_related_account
		  	DBMS_STATS.gather_table_stats (
				ownname    => 'IBS',
				tabname    => 'sync_etl_cdmast'
            ); 
--            DBMS_STATS.gather_table_stats (
--                ownname    => 'IBS',
--                tabname    => 'bc_related_account'
--            ); 
        -- end    
        SELECT MAX(a.cifno) + 1 --#20150119 Loctx chagne + 1
        INTO   l_max_cif
        FROM   sync_etl_cdmast a;

        SELECT MIN(a.cifno)
        INTO   l_min_count
        FROM   sync_etl_cdmast a;

        IF (l_max_cif IS NULL)
        THEN
          l_max_cif := 0;
        END IF;

        IF (l_min_count IS NULL)
        THEN
          l_min_count := 0;
        END IF;


   /* INSERT INTO sync_cif_status
      SELECT a.cif_no,
             'Y'
      FROM   (SELECT cif_no
              FROM   bc_user_info
              UNION
              SELECT cif_no
              FROM   bb_corp_info) a;*/

    LOOP
        BEGIN
            l_max_count := l_min_count + g_limit_count;
            MERGE INTO   bk_receipt_info c
                 USING   (SELECT
                                (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                   b.cdterm,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   (case when length(b.cdnum) = 13 then LPAD (b.cdnum, 14, '0') else TO_CHAR(b.cdnum) end) cdnum,
                                   DECODE(issdt,0,null,TO_DATE (b.issdt, 'yyyyddd')) issdt,
                                   DECODE(matdt,0,null,TO_DATE (b.matdt, 'yyyyddd')) matdt,
                                   fn_get_account_status_code(b.status) status,
                                   nvl(rp.product_code, TRIM(b.TYPE)) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   b.rate,
                                   b.brn
                            FROM   sync_etl_cdmast b
                            LEFT join BK_RECEIPT_PRODUCT rp
                            ON b.TYPE = rp.core_product_code
                               and rp.status = 'ACTV'  
                               and rp.term = b.CDTERM 
                               and rp.term_code = b.CDTCOD                               
                            /*,
                                    (SELECT distinct cif_no
                                      FROM   bc_user_info
                                      UNION
                                      SELECT distinct cif_no
                                      FROM   bb_corp_info
                                      )cif*/--#20150409 Loctx disable vi loi cap nhat lai cif
                           WHERE       TRIM (b.TYPE) IS NOT NULL
                                   AND b.acctno <> 0
                                   AND b.status > 0 --#20150119 HaoNS changefrom status is not null
                                   --AND cif.cif_no = TO_CHAR(b.cifno)
                                   --#20150119 Loctx change
                                   --AND b.cifno BETWEEN l_min_count AND  l_max_count
                                   AND b.cifno >= l_min_count AND b.cifno < l_max_count  --#20150119 change
                                   -- 20190716, ChiPM loc cac tai khoan dong so huu o bang cdc_cfacct
                                  --AND NOT EXISTS (select 1 from cdc_cfacct where cdc_cfacct.CFACCN = b.acctno) 
                                  AND NOT EXISTS (select 1 from cdc_cfacct where cdc_cfacct.CFACCN = b.acctno
                                    and cdc_cfacct.cfcifn not in ( select cif_no from bk_cif_whitelist ) )
                                    /*
                                   -- 20150701 QuanPD add tam thoi: loai bo san pham MDB
                                   AND NOT EXISTS (SELECT * FROM ibs.cstb_branch_mdb_loai_tk mdb
                                                    WHERE mdb.branch_code = b.brn)
                                    */
                         ) a
                    ON   (a.acctno = c.receipt_no)
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.product_code = a.product_type,
                    c.principal = a.cbal,                      --a.orgbal,
                    c.interest_rate = a.rate,
                    c.interest_amount = a.accint,
                    c.term = a.cdterm,
                    c.opening_date = a.issdt,
                    c.settlement_date = a.matdt,
                    c.is_rollout_interest = a.renew,
                    c.interest_receive_account = a.dactn,
                    c.status = a.status,
                    c.account_no = a.cdnum
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
                    VALUES   (a.acctno,
                              a.product_type,
                              a.cdnum,
                              a.cbal,
                              a.rate,
                              a.accint,
                              a.cdterm,
                              a.issdt,
                              a.matdt,
                              a.renew,
                              a.dactn,
                              a.status);

            COMMIT;

            MERGE INTO   bk_account_info c
                 USING   (SELECT
                                (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                (case when length(b.brn) <= 2 then LPAD (b.brn, 3, '0') else TO_CHAR(b.brn) end) brn,
                                   b.cifno,
                                   b.cdterm,
                                   TRIM (b.curtyp) curtyp,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   (case when length(b.cdnum) = 13 then LPAD (b.cdnum, 14, '0') else TO_CHAR(b.cdnum) end) cdnum,
                                   b.penamt,
                                   b.hold,
                                   DECODE(b.issdt,0,null,TO_DATE ( b.issdt, 'yyyyddd')) issdt,
                                   DECODE(b.matdt,0,null,TO_DATE (b.matdt, 'yyyyddd')) matdt,
                                   trim(b.acname) acname,
                                   fn_get_account_status_code(b.status) status,
                                   nvl(b.eb_product_code, TRIM(b.TYPE)) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   TRIM(b.rate) rate,
                                   b.wdrwh
                            FROM   (select a.*, rp.product_code as eb_product_code from 
                                    sync_etl_cdmast a
                                    LEFT join BK_RECEIPT_PRODUCT rp
                                    ON a.TYPE = rp.core_product_code
                                       and rp.status = 'ACTV'  
                                       and rp.term = a.cdterm 
                                       and rp.term_code = a.cdtcod
                                    -- 20190716, ChiPM loc cac tai khoan dong so huu o bang cdc_cfacct
                                    --WHERE NOT EXISTS (select 1 from cdc_cfacct where cdc_cfacct.CFACCN = sync_etl_cdmast.acctno)	
                                    WHERE NOT EXISTS (select 1 from cdc_cfacct a where a.CFACCN = a.acctno
                                    and a.cfcifn not in ( select cif_no from bk_cif_whitelist )  )
                                    ) b,(
                                                          SELECT distinct cif_no
                                                          FROM   bb_corp_info)cif
                            
                           WHERE       TRIM (b.TYPE) IS NOT NULL
                                   AND b.acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                                   AND  cif.cif_no = TRIM (b.cifno)
                                   --AND b.cifno BETWEEN l_min_count AND  l_max_count
                                   AND b.cifno >= l_min_count AND b.cifno < l_max_count
                         ) a
                    ON   (a.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET c.bank_no = '302',
                           c.org_no = a.brn,
                           c.cif_no = a.cifno,
                           c.acct_type = 'FD',
                           c.currency_code = a.curtyp,
                           c.original_balance = a.orgbal,
                           c.principal_balance = a.cbal,
                           c.accured_interest = a.accint,
                           c.p_acct_no = a.cdnum,
                           c.penalty_amount = a.penamt,
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
                                     c.status  )
                    VALUES   (a.acctno,
                              '302',
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
                              a.status );

                COMMIT;


                l_min_count := l_max_count;

            --khong co them ban ghi nao
                EXIT WHEN (l_max_count > l_max_cif);
            END;
        END LOOP;

        --#20170726 Loctx add for fix loi thay doi CIF nhung cif nay khong su dung EBANK

            MERGE INTO   bk_account_info c
                 USING   (SELECT    info.rowid rid,
                            (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                            (case when length(b.brn) <= 2 then LPAD (b.brn, 3, '0') else TO_CHAR(b.brn) end) brn,
                                   b.cifno,
                                   b.cdterm,
                                   TRIM (b.curtyp) curtyp,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   (case when length(b.cdnum) = 13 then LPAD (b.cdnum, 14, '0') else TO_CHAR(b.cdnum) end) cdnum,
                                   b.penamt,
                                   b.hold,
                                   DECODE(b.issdt,0,null,TO_DATE ( b.issdt, 'yyyyddd')) issdt,
                                   DECODE(b.matdt,0,null,TO_DATE (b.matdt, 'yyyyddd')) matdt,
                                   trim(b.acname) acname,
                                   fn_get_account_status_code(b.status) status,
                                   nvl(b.eb_product_code, TRIM(b.TYPE)) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   TRIM(b.rate) rate,
                                   b.wdrwh
                            FROM   (select a.*, b. rp.product_code as eb_product_code from sync_etl_cdmast a 
                                    LEFT join BK_RECEIPT_PRODUCT rp
                                    ON a.TYPE = rp.core_product_code
                                       and rp.status = 'ACTV'  
                                       and rp.term = a.cdterm 
                                       and rp.term_code = a.cdtcod
                            -- 20190716, ChiPM loc cac tai khoan dong so huu o bang cdc_cfacct
                            --WHERE NOT EXISTS (select 1 from cdc_cfacct where cdc_cfacct.CFACCN = sync_etl_cdmast.acctno)	
                            WHERE NOT EXISTS (select 1 from cdc_cfacct where cdc_cfacct.CFACCN = a.acctno
                            and cdc_cfacct.cfcifn not in ( select cif_no from bk_cif_whitelist )  )
                            ) b, bk_account_info info
                           WHERE       TRIM (b.TYPE) IS NOT NULL
                                   AND b.acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                                   AND (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) = info.acct_no
                                   AND NOT EXISTS
                                   (SELECT 1 FROM
                                            (
                                                          SELECT distinct cif_no
                                                          FROM   bb_corp_info
                                            )cif
                                    WHERE cif.cif_no =  TRIM (b.cifno)
                                    )

                         ) a
                    ON   (a.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET c.bank_no = '302',
                           c.org_no = a.brn,
                           c.cif_no = a.cifno,
                           c.acct_type = 'FD',
                           c.currency_code = a.curtyp,
                           c.original_balance = a.orgbal,
                           c.principal_balance = a.cbal,
                           c.accured_interest = a.accint,
                           c.p_acct_no = a.cdnum,
                           c.penalty_amount = a.penamt,
                           c.hold_amount = a.hold,
                           c.issued_date = a.issdt,
                           c.maturity_date = a.matdt,
                           c.acct_name = a.acname,
                           c.product_type = a.product_type,
                           c.interest_rate = a.rate,
                           c.status = a.status
                           ;

--        SELECT acct_no
--        BULK COLLECT
--        INTO   l_acct_cls
--        FROM   bc_related_account a
--        WHERE  a.status = 'CLOS';

--        FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
--          DELETE FROM bc_related_account a
--          WHERE  a.acct_no = l_acct_cls(i);

--        COMMIT;

        --clear
        l_acct_cls.delete;

        SELECT receipt_no BULK COLLECT
        INTO   l_acct_cls
        FROM   bk_receipt_info a
        WHERE  a.status = 'CLOS';

        --#20160427 Loctx add theo y/c cua Luong de tracking  update comment  bk_receipt_info_close 08/10/2018

        /*FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          INSERT INTO bk_receipt_info_close(
            receipt_no, product_code, account_no, principal,
            interest_rate, interest_amount, term, opening_date,
            settlement_date, is_rollout_interest,
            interest_receive_account, status, cdtcod
          )
          SELECT receipt_no, product_code, account_no, principal,
            interest_rate, interest_amount, term, opening_date,
            settlement_date, is_rollout_interest,
            interest_receive_account, status, cdtcod
        FROM bk_receipt_info
        WHERE  receipt_no = l_acct_cls(i); */

        --tam thoi delete cac tai khoan da tat toan
        FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          DELETE FROM bk_receipt_info a
          WHERE  a.receipt_no = l_acct_cls(i);

        COMMIT;

        l_acct_cls.delete;

        SELECT acct_no BULK COLLECT
        INTO   l_acct_cls
        FROM   bk_account_info a
        WHERE  a.status = 'CLOS'
        AND    a.acct_type = 'FD';

        --#20160427 Loctx add theo y/c cua Luong de tracking update comment  b? bk_account_info_close 08/10/2018
       /* FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          INSERT INTO bk_account_info_close(
            acct_no, acct_name, p_acct_no, bank_no, org_no,
            product_type, loan_no, branch_no, sub_branch_no,
            cb_sign, cert_type, cert_code, acct_type,
            sub_acct_type, currency_code, cx_sign, establish_date,
            available_date, current_balance, freezed_balance,
            available_balance, remark, status, create_by,
            create_time, update_by, update_time, cif_no,
            ledger_balance, hold_amount, overdraft_limit,
            interest_rate, date_account_opened, passbook_no,
            accured_interest, term, original_balance,
            principal_balance, penalty_amount, current_cash_value,
            earmarked_amount, issued_date, maturity_date,
            times_nenewed_count, purpose_code, loan_original_amt,
            os_principal, os_balance, loan_term, principal_frequent,
            interest_frequent, full_release_date,
            overdue_indicator_description, billed_total_amount,
            billed_principal, billed_interest, billed_late_charge,
            payment_amount, final_payment_amount, accrued_late_charge,
            accrued_common_fee, other_charges, available_limit,
            tc_code_loan, last_run_time_loan
          )
          SELECT acct_no, acct_name, p_acct_no, bank_no, org_no,
            product_type, loan_no, branch_no, sub_branch_no,
            cb_sign, cert_type, cert_code, acct_type,
            sub_acct_type, currency_code, cx_sign, establish_date,
            available_date, current_balance, freezed_balance,
            available_balance, remark, status, create_by,
            create_time, update_by, update_time, cif_no,
            ledger_balance, hold_amount, overdraft_limit,
            interest_rate, date_account_opened, passbook_no,
            accured_interest, term, original_balance,
            principal_balance, penalty_amount, current_cash_value,
            earmarked_amount, issued_date, maturity_date,
            times_nenewed_count, purpose_code, loan_original_amt,
            os_principal, os_balance, loan_term, principal_frequent,
            interest_frequent, full_release_date,
            overdue_indicator_description, billed_total_amount,
            billed_principal, billed_interest, billed_late_charge,
            payment_amount, final_payment_amount, accrued_late_charge,
            accrued_common_fee, other_charges, available_limit,
            tc_code_loan, last_run_time_loan
        FROM bk_account_info
        WHERE  acct_no = l_acct_cls(i);*/

        FORALL i IN l_acct_cls.FIRST .. l_acct_cls.LAST
          DELETE FROM bk_account_info a
          WHERE  a.acct_no = l_acct_cls(i);

        COMMIT;
        -- 11/02/2020: update de loc cac tai khoan FD GROUP dong so huu
        update bk_account_info set status = 'DLTD' where acct_no in (
        select (case when length(cfaccn) = 13 then LPAD (cfaccn, 14, '0') else TO_CHAR(cfaccn) end) from cdc_cfacct
        );commit; 
        -- update bk_receipt_info set status = 'CLOS' where ACCOUNT_NO in (select lpad(cfaccn, 14 , '0') from cdc_cfacct);commit; 
        -- 11/02/2020.
        plog.setendsection( pkgctx, 'pr_cdmast_sync' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc || 'l_min_count = ' || l_min_count || ',l_max_count=' || l_max_count);
            plog.setendsection( pkgctx, 'pr_cdmast_sync' );
            --forward error
            RAISE;

    END;
------------------------------------------------------------------------------------------------------
 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  DuongDV     08/12/2014   Created

----------------------------------------------------------------------------------------------------*/
PROCEDURE pr_cdmemo_cdc_override
    IS
        l_acc_no VARCHAR2(14);
        l_err_desc VARCHAR2(250);
    BEGIN
        MERGE INTO bk_receipt_info c
        USING
        (
            select
                (case when length(acctno) = 13 then LPAD (acctno, 14, '0') else TO_CHAR(acctno) end) as receipt_no,
                cbal as principal,
                accint as interest_amount,
                fn_get_account_status_code(status) as status
                from RAWSTAGE.si_dat_cdmemo@RAWSTAGE_PRO_CORE
                WHERE acctno <> 0
                        and status IS NOT NULL

        )src
        ON (src.receipt_no = c.receipt_no)
        WHEN MATCHED THEN
        UPDATE
        SET
                     c.principal = src.principal,
                     c.interest_amount = src.interest_amount,
                     c.status = src.status
;
        MERGE INTO bk_account_info c
        USING
        (
            select
                    cbal as original_balance,
                    cbal as principal_balance,
                    accint as accured_interest,
                    penamt as penalty_amount,
                    hold as hold_amount,
                    (cbal + accint - penamt - hold - wdrwh) as current_cash_value,
                    fn_get_account_status_code(status) as status,
                    'FD' as acct_type,
                    cdnum as p_acct_no,
                    (case when length(acctno) = 13 then LPAD (acctno, 14, '0') else TO_CHAR(acctno) end) as acct_no
            from RAWSTAGE.si_dat_cdmemo@RAWSTAGE_PRO_CORE
            where
                    acctno <> 0
                        and status IS NOT NULL
        ) src
        ON(src.acct_no  = c.acct_no)
        WHEN MATCHED THEN
        UPDATE
        SET
                    c.original_balance = src.original_balance,
                    c.principal_balance  = src.principal_balance,
                    c.accured_interest   = src.accured_interest,--#20141316 Loctx co the can check lai khi co phat sinh FD khi chay batch
                    c.penalty_amount     = src.penalty_amount,
                    c.hold_amount        = src.hold_amount,
                    c.current_cash_value = src.current_cash_value,
                    c.status = src.status,
                    c.acct_type = src.acct_type,
                    c.p_acct_no = src.p_acct_no
;

        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

          --cspks_cdc_util.pr_log_sync_error('SIBS', 'CDMEMO', 'BK_ACCOUNT_INFO',
           --                                       orchestrate_acctno, dm_operation_type, l_err_desc);
          --COMMIT;
          --RAISE;

  END;
------------------------------------------------------------------------------------------------------
    PROCEDURE pr_passbook_no_sync
    IS

        l_max_cif NUMBER;
        l_min_count NUMBER;
        l_max_count NUMBER;

        l_error_desc VARCHAR2(300);

    BEGIN
        plog.setbeginsection( pkgctx, 'pr_passbook_no_sync' );

        l_max_count   := 0;


        SELECT MAX(a.tmbmrser) + 1
        INTO   l_max_cif
        FROM   sync_etl_tmpbmast a;

        SELECT MIN(a.tmbmrser)
        INTO   l_min_count
        FROM   sync_etl_tmpbmast a;

        IF (l_max_cif IS NULL)
        THEN
         l_max_cif := 0;
        END IF;

        IF (l_min_count IS NULL)
        THEN
            l_min_count := 0;
        END IF;

        INSERT INTO sync_account_info
            (SELECT bai.cif_no, bai.acct_no
               FROM bk_account_info bai
              WHERE bai.cif_no IN (
                                   SELECT cif_no FROM bb_corp_info));

--Comment 20/12/2023: khong co mapping
       -- EXECUTE IMMEDIATE ' TRUNCATE TABLE SYNC_ETL_TMPBMAST';
       -- INSERT INTO sync_etl_tmpbmast
       --  SELECT
       --          a.tmbmacct,
       --          a.tmbmrser
-- 
       --    FROM   svdatpv51.tmpbmast@dblink_data a
       --    WHERE  LPAD(a.tmbmacct,
       --                14,
       --                '0') IN (SELECT acct_no
       --                         FROM   sync_account_info);
        Commit;
        -- comment 16/10/2018 
--        DBMS_STATS.gather_table_stats (
--          	ownname    => 'IBS',
--          	tabname    => 'sync_etl_tmpbmast'
--		     	);
--        -- comment 16/10/2018 
--        DBMS_STATS.gather_table_stats (
--              ownname    => 'IBS',
--              tabname    => 'bk_account_info'
--                 );
       -- comment 16/10/2018
        LOOP
            l_max_count := l_min_count + g_limit_count;

            MERGE INTO bk_account_info c
             USING (SELECT TO_CHAR(b.tmbmrser) tmbmrser, b.tmbmacct
                      FROM sync_etl_tmpbmast b,
                      (
                        SELECT bai.cif_no, bai.acct_no
                        FROM bk_account_info bai
                        /*,
                        (
                            SELECT DIS cif_no FROM bc_user_info
                            UNION
                            SELECT cif_no FROM bb_corp_info
                        ) d
                        WHERE d.cif_no = bai.cif_no
                        */
                      )c
                     WHERE c.acct_no = (case when length(b.tmbmacct) = 13 then LPAD (b.tmbmacct, 14, '0') else TO_CHAR(b.tmbmacct) end)
                    --AND b.tmbmrser BETWEEN l_min_count AND  l_max_count
                    AND b.tmbmrser >= l_min_count AND  b.tmbmrser <= l_max_count--#20150121 Loctx comment
                   )a
                    ON (a.tmbmacct = TO_NUMBER(c.acct_no))
            WHEN MATCHED THEN
                UPDATE SET c.passbook_no = a.tmbmrser;

            COMMIT;

            l_min_count := l_max_count;
              --khong co them ban ghi nao
            EXIT WHEN (l_max_count > l_max_cif);
        END LOOP;

        plog.setendsection( pkgctx, 'pr_passbook_no_sync' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc || 'l_min_count = ' || l_min_count || ',l_max_count=' || l_max_count);
            plog.setendsection( pkgctx, 'pr_passbook_no_sync' );
            --forward error
            RAISE;

    END;

BEGIN
    SELECT   *
    INTO   logrow
    FROM   tlogdebug
    WHERE   ROWNUM <= 1;

    pkgctx      :=
    plog.init('cspkg_account_etl_sync',
        plevel => logrow.loglevel,
        plogtable => ( logrow.log4table = 'Y' ),
        palert => ( logrow.log4alert = 'Y' ),
        ptrace => ( logrow.log4trace = 'Y' ) );
END;

/
