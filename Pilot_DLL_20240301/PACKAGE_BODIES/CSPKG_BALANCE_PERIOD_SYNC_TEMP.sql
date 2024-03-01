--------------------------------------------------------
--  DDL for Package Body CSPKG_BALANCE_PERIOD_SYNC_TEMP
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."CSPKG_BALANCE_PERIOD_SYNC_TEMP" 
IS

    pkgctx    plog.log_ctx;
    logrow    tlogdebug%ROWTYPE;

  /*----------------------------------------------------------------------------------------------------
  ** Description:
  **  Xu ly truong hop du lieu cap nhat dong thoi vao ddmemo
  **  Chay hang ngay
  **  Person      Date           Comments
  **  QuanPD     20/08/2015      Created
  ----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_ddmemo_later_process
    IS
        l_today7 NUMBER(7);
    BEGIN
          SELECT param_value
          INTO l_today7
          FROM cstb_system
          WHERE  param_name = 'CUR_DATE7';

          MERGE INTO bk_account_info a
          USING
                (SELECT
                        bka.acct_no acct_no,
                        bka.acct_type acct_type,
                        memo.cbal ledger_balance,
                        memo.hold hold_amount,
                        fn_get_account_status_code (memo.status) status,
                        memo.cbal - memo.hold + memo.odlimt available_balance,
                        memo.odlimt overdraft_limit,
                        TRIM (memo.acname) acct_name
                FROM    bk_account_info bka,
                        --RAWSTAGE.si_dat_ddmemo@RAWSTAGE_PRO memo
                        SVDATPV51.DDMEMO@DBLINK_DATA memo --#20160611 Loctx change tam thoi gi RAWSTAGE LOI JOB RWSIDD
                WHERE   bka.acct_no = LPAD (memo.acctno, 14, '0')
                AND     bka.acct_type = fn_get_actype_code (TRIM(memo.actype))
                AND     (bka.ledger_balance <> memo.cbal
                        OR bka.hold_amount <> memo.hold
                        OR bka.status <> fn_get_account_status_code (memo.status)
                        OR bka.available_balance <> memo.cbal - memo.hold + memo.odlimt
                        OR bka.overdraft_limit <> memo.odlimt
                        OR bka.acct_name <> TRIM (memo.acname))

                )src
          ON (a.acct_no = src.acct_no
              AND a.acct_type = src.acct_type)
          WHEN MATCHED THEN
          UPDATE
                SET
                    a.ledger_balance = src.ledger_balance,
                    a.hold_amount = src.hold_amount,
                    a.status = src.status,
                    a.available_balance = src.available_balance,
                    a.overdraft_limit = src.overdraft_limit,
                    a.acct_name = src.acct_name;
          COMMIT;

    END pr_ddmemo_later_process;

    PROCEDURE pr_cdmemo_later_process
    IS
        l_today7 NUMBER(7);
    BEGIN
          SELECT param_value
          INTO l_today7
          FROM cstb_system
          WHERE  param_name = 'CUR_DATE7';

          MERGE INTO bk_account_info a
          USING
                (SELECT
                        bka.acct_no acct_no,
                        bka.acct_type acct_type,
                        memo.cbal original_balance,
                        memo.cbal principal_balance,
                        memo.penamt penalty_amount,
                        memo.hold hold_amount,
                        (memo.cbal + memo.accint - memo.penamt - memo.hold - memo.wdrwh) current_cash_value,
                        fn_get_account_status_code (memo.status) status,
                        LPAD(memo.cdnum, 14, '0') p_acct_no
                FROM    ibs.bk_account_info bka,
                        --RAWSTAGE.si_dat_cdmemo@RAWSTAGE_PRO memo
                        SVDATPV51.cDMEMO@DBLINK_DATA memo --#20160611 Loctx change tam thoi gi RAWSTAGE LOI JOB RWSIDD
                WHERE   bka.acct_no = LPAD (memo.acctno, 14, '0')
                AND     bka.acct_type = 'FD'
                AND     (bka.hold_amount <> memo.hold
                        OR bka.status <> fn_get_account_status_code (memo.status)
                        OR bka.principal_balance <> memo.cbal
                        OR bka.penalty_amount <> memo.penamt
                        OR bka.p_acct_no <> memo.cdnum)

                )src
          ON (a.acct_no = src.acct_no
              AND a.acct_type = src.acct_type)
          WHEN MATCHED THEN
          UPDATE
                SET
                    a.original_balance = src.original_balance,
                    a.principal_balance = src.principal_balance,
                    a.penalty_amount = src.penalty_amount,
                    a.hold_amount = src.hold_amount,
                    a.current_cash_value = src.current_cash_value,
                    a.status = src.status,
                    a.p_acct_no = src.p_acct_no;
            ---

            MERGE INTO bk_receipt_info a
          USING
                (SELECT
                        bkr.receipt_no receipt_no,
                        memo.cbal principal,
                        fn_get_account_status_code (memo.status) status
                FROM    bk_receipt_info bkr,
                        rawstage.si_dat_cdmemo@RAWSTAGE_PRO memo
                WHERE   bkr.receipt_no = LPAD (memo.acctno, 14, '0')
                AND     (bkr.status <> fn_get_account_status_code (memo.status)
                        OR bkr.principal <> memo.cbal)
                )src
          ON (a.receipt_no = src.receipt_no)
          WHEN MATCHED THEN
          UPDATE
                SET
                    a.principal = src.principal,
                    a.status = src.status;

          COMMIT;

    END pr_cdmemo_later_process;

        BEGIN
        SELECT   *
        INTO   logrow
        FROM   tlogdebug
        WHERE   ROWNUM <= 1;

        pkgctx      :=
        plog.init('acpkg_cdaccount_cdc',
            plevel => logrow.loglevel,
            plogtable => ( logrow.log4table = 'Y' ),
            palert => ( logrow.log4alert = 'Y' ),
            ptrace => ( logrow.log4trace = 'Y' ) );

END;

/
