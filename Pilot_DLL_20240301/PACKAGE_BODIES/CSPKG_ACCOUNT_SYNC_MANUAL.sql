--------------------------------------------------------
--  DDL for Package Body CSPKG_ACCOUNT_SYNC_MANUAL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_ACCOUNT_SYNC_MANUAL" is

 PROCEDURE pr_ddmemo_sync
    IS
        l_today7 NUMBER(7);
        sys_status VARCHAR(10);
    BEGIN
          SELECT param_value
          INTO l_today7
          FROM cstb_system
          WHERE  param_name = 'CUR_DATE7';
          
          SELECT code INTO sys_status FROM Bk_Sys_Config where name='sys_status';
      -- Day Mode
        IF sys_status='on' THEN 
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
          
       -- Night Mode
       ELSE
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
                        svdatp24h.ddme24@DBLINK_DATA memo --#20160611 Loctx change tam thoi gi RAWSTAGE LOI JOB RWSIDD
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
       END IF;

    END pr_ddmemo_sync;
    
    -------------------------------------------------------------------
    -------------------------------------------------------------------
    PROCEDURE pr_ddmemo_sync_single (p_acct_no VARCHAR2)
    AS
        sys_status VARCHAR(10);
    BEGIN
          
          SELECT code INTO sys_status FROM Bk_Sys_Config where name='sys_status';
      -- Day Mode
        IF sys_status='on' THEN 
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
                AND     memo.acctno = p_acct_no
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
          
       -- Night Mode
       ELSE
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
                        svdatp24h.ddme24@DBLINK_DATA memo --#20160611 Loctx change tam thoi gi RAWSTAGE LOI JOB RWSIDD
                WHERE   bka.acct_no = LPAD (memo.acctno, 14, '0')
                AND     memo.acctno = p_acct_no
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
       END IF;

    END pr_ddmemo_sync_single;
    -------------------------------------------------------------------
    -------------------------------------------------------------------

    PROCEDURE pr_cdmemo_sync
    IS
        l_today7 NUMBER(7);
        sys_status VARCHAR(10);
    BEGIN
          SELECT param_value
          INTO l_today7
          FROM cstb_system
          WHERE  param_name = 'CUR_DATE7';
          
          SELECT code INTO sys_status FROM Bk_Sys_Config where name='sys_status';
      ------ Day Mode ------
        IF sys_status='on' THEN 

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
                        SVDATPV51.CDMEMO@DBLINK_DATA memo --#202004131 Loctx change
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
                        fn_get_account_status_code (TRIM(memo.status)) status
                FROM    bk_receipt_info bkr,
                        --rawstage.si_dat_cdmemo@RAWSTAGE_PRO memo
                        SVDATPV51.CDMEMO@DBLINK_DATA memo
                WHERE   bkr.receipt_no = LPAD (TRIM(memo.acctno), 14, '0')
                AND     (bkr.status <> fn_get_account_status_code (TRIM(memo.status))
                        OR bkr.principal <> memo.cbal)
                )src
          ON (a.receipt_no = src.receipt_no)
          WHEN MATCHED THEN
          UPDATE
                SET
                    a.principal = src.principal,
                    a.status = src.status;

          COMMIT;
          
        ELSE
          ------ Night Mode ------
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
                        svdatp24h.cdme24@DBLINK_DATA memo --#202004131 Loctx change
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
                        fn_get_account_status_code (TRIM(memo.status)) status
                FROM    bk_receipt_info bkr,
                        --rawstage.si_dat_cdmemo@RAWSTAGE_PRO memo
                        svdatp24h.cdme24@DBLINK_DATA memo
                WHERE   bkr.receipt_no = LPAD (TRIM(memo.acctno), 14, '0')
                AND     (bkr.status <> fn_get_account_status_code (TRIM(memo.status))
                        OR bkr.principal <> memo.cbal)
                )src
          ON (a.receipt_no = src.receipt_no)
          WHEN MATCHED THEN
          UPDATE
                SET
                    a.principal = src.principal,
                    a.status = src.status;

          COMMIT;
        END IF;

    END pr_cdmemo_sync;
    -------------------------------------------------------------------
    -------------------------------------------------------------------
    PROCEDURE pr_cdmemo_sync_single (p_acct_no VARCHAR2)
    AS
        sys_status VARCHAR(10);
    BEGIN
          
          SELECT code INTO sys_status FROM Bk_Sys_Config where name='sys_status';
      ------ Day Mode ------
        IF sys_status='on' THEN 

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
                        SVDATPV51.CDMEMO@DBLINK_DATA memo --#202004131 Loctx change
                WHERE   bka.acct_no = LPAD (memo.acctno, 14, '0')
                AND     memo.acctno = p_acct_no
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
                        fn_get_account_status_code (TRIM(memo.status)) status
                FROM    bk_receipt_info bkr,
                        --rawstage.si_dat_cdmemo@RAWSTAGE_PRO memo
                        SVDATPV51.CDMEMO@DBLINK_DATA memo
                WHERE   bkr.receipt_no = LPAD (TRIM(memo.acctno), 14, '0')
                AND     memo.acctno = p_acct_no
                AND     (bkr.status <> fn_get_account_status_code (TRIM(memo.status))
                        OR bkr.principal <> memo.cbal)
                )src
          ON (a.receipt_no = src.receipt_no)
          WHEN MATCHED THEN
          UPDATE
                SET
                    a.principal = src.principal,
                    a.status = src.status;

          COMMIT;
          
        ELSE
          ------ Night Mode ------
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
                        svdatp24h.cdme24@DBLINK_DATA memo --#202004131 Loctx change
                WHERE   bka.acct_no = LPAD (memo.acctno, 14, '0')
                AND     memo.acctno = p_acct_no
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
                        fn_get_account_status_code (TRIM(memo.status)) status
                FROM    bk_receipt_info bkr,
                        --rawstage.si_dat_cdmemo@RAWSTAGE_PRO memo
                        svdatp24h.cdme24@DBLINK_DATA memo
                WHERE   bkr.receipt_no = LPAD (TRIM(memo.acctno), 14, '0')
                AND     memo.acctno = p_acct_no
                AND     (bkr.status <> fn_get_account_status_code (TRIM(memo.status))
                        OR bkr.principal <> memo.cbal)
                )src
          ON (a.receipt_no = src.receipt_no)
          WHEN MATCHED THEN
          UPDATE
                SET
                    a.principal = src.principal,
                    a.status = src.status;

          COMMIT;
        END IF;

    END pr_cdmemo_sync_single;
    
    -------------------------------------------------------------------
    -------------------------------------------------------------------

 PROCEDURE pr_ddtnew_sync
    IS
        cursor c_ddtnew
        is
           select branch,acctno,actype,ddctyp,cifno,status,hold,cbal,odlimt,rate,acname,sccode,datop7,accrue 
           from (
              select (case when b.acct_no is null then 'I' ELSE 'U' END) dm_operation_type, 
                     TRIM(a.branch) branch,a.acctno,a.actype,a.ddctyp,a.cifno,TRIM(a.status) status,
                     a.hold,a.cbal,a.odlimt,a.rate,a.acname,a.sccode,TO_NUMBER(TRIM(a.datop7)) datop7,a.accrue
              --from SVDATPV51.DDTNEW@DBLINK_DATA a 
              from RAWSTAGE.SI_DAT_DDTNEW@RAWSTAGE_PRO a
              left join ibs.bk_account_info b on a.acctno=b.acct_no
              ) where dm_operation_type='I';
           
        type ty_data is table of c_ddtnew%rowtype index by PLS_INTEGER;
        l_ddtnew_list ty_data;
    BEGIN
      open c_ddtnew;
      LOOP
        FETCH c_ddtnew
        BULK COLLECT INTO l_ddtnew_list
        LIMIT  10000;
        
        FOR i IN 1..l_ddtnew_list.count
        LOOP
          CSPKG_ACCOUNT_SYNC.pr_ddtnew_sync('I',
                                            null,
                                            l_ddtnew_list(i).branch,
                                            l_ddtnew_list(i).acctno,
                                            l_ddtnew_list(i).actype,
                                            l_ddtnew_list(i).ddctyp,
                                            l_ddtnew_list(i).cifno,
                                            l_ddtnew_list(i).status,
                                            l_ddtnew_list(i).hold,
                                            l_ddtnew_list(i).cbal,
                                            l_ddtnew_list(i).odlimt,
                                            l_ddtnew_list(i).rate,
                                            l_ddtnew_list(i).acname,
                                            l_ddtnew_list(i).sccode,
                                            l_ddtnew_list(i).datop7,
                                            l_ddtnew_list(i).accrue
                                            );
        END LOOP;
 
        EXIT WHEN c_ddtnew%NOTFOUND;

      END LOOP;
      CLOSE c_ddtnew ;
      
     exception
      when others THEN
          CLOSE c_ddtnew;
          raise;

    END;
    
    --- CDTNEW
    PROCEDURE pr_cdtnew_sync
    IS
        cursor c_cdtnew
        is
           select bankno,brn,curtyp,cifno,orgbal,cbal,accint,penamt,hold,wdrwh,cdnum,issdt,matdt,rnwctr,status,acname,acctno,type,rate,renew,dactn,cdterm,cdmuid 
           from (
              select (case when b.acct_no is null then 'I' ELSE 'U' END) dm_operation_type, 
                     a.bankno,a.brn,a.curtyp,a.cifno,a.orgbal,a.cbal,a.accint,a.penamt,a.hold,a.wdrwh,a.cdnum,a.issdt,
                     a.matdt,a.rnwctr,a.status,a.acname,a.acctno,a.type,a.rate,a.renew,a.dactn,a.cdterm,a.cdmuid
              from RAWSTAGE.SI_DAT_CDTNEW@RAWSTAGE_PRO a
              left join ibs.bk_account_info b on a.acctno=b.acct_no
              ) where dm_operation_type='I';
           
        type ty_data is table of c_cdtnew%rowtype index by PLS_INTEGER;
        l_cdtnew_list ty_data;
    BEGIN
      open c_cdtnew;
      LOOP
        FETCH c_cdtnew
        BULK COLLECT INTO l_cdtnew_list
        LIMIT  10000;
        
        FOR i IN 1..l_cdtnew_list.count
        LOOP
          CSPKG_ACCOUNT_SYNC.pr_cdtnew_sync('I',
                                            l_cdtnew_list(i).bankno,
                                            l_cdtnew_list(i).brn,
                                            l_cdtnew_list(i).curtyp,
                                            l_cdtnew_list(i).cifno,
                                            l_cdtnew_list(i).orgbal,
                                            l_cdtnew_list(i).cbal,
                                            l_cdtnew_list(i).accint,
                                            l_cdtnew_list(i).penamt,
                                            l_cdtnew_list(i).hold,
                                            l_cdtnew_list(i).wdrwh,
                                            l_cdtnew_list(i).cdnum,
                                            l_cdtnew_list(i).issdt,
                                            l_cdtnew_list(i).matdt,
                                            l_cdtnew_list(i).rnwctr,
                                            l_cdtnew_list(i).status,
                                            l_cdtnew_list(i).acname,
                                            l_cdtnew_list(i).acctno,
                                            l_cdtnew_list(i).type,
                                            l_cdtnew_list(i).rate,
                                            l_cdtnew_list(i).renew,
                                            l_cdtnew_list(i).dactn,
                                            l_cdtnew_list(i).cdterm,
                                            l_cdtnew_list(i).cdmuid
                                            );
        END LOOP;
 
        EXIT WHEN c_cdtnew%NOTFOUND;

      END LOOP;
      CLOSE c_cdtnew ;
      
     exception
      when others THEN
          CLOSE c_cdtnew;
          raise;

    END;

end CSPKG_ACCOUNT_SYNC_MANUAL;

/
