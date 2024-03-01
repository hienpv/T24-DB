--------------------------------------------------------
--  DDL for Package Body EBANK_ACCOUNT_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."EBANK_ACCOUNT_SYNC" 
/* Formatted on 11-Dec-2011 0:58:14 (QP5 v5.126) */
 IS
  v_start_date DATE;

  TYPE rowid_list IS TABLE OF UROWID INDEX BY PLS_INTEGER;

  TYPE acct_list IS TABLE OF VARCHAR2(50) INDEX BY BINARY_INTEGER;

  min_rid_tab rowid_list;

  max_rid_tab rowid_list;

  g_error_level NUMBER;

  g_loop_count NUMBER;

  g_limit_count NUMBER;

  g_min_count NUMBER;

  g_max_count NUMBER;

  g_cif_count NUMBER;

  g_error_count NUMBER;

  g_error_sub_count NUMBER;

  acct_bkr_rpno_len_tab acct_list;

  acct_bkr_acno_len_tab acct_list;

  acct_bkr_st_tab acct_list;

  acct_bkr_st_clos_tab acct_list;

  acct_bka_st_clos_tab acct_list;

  acct_bcr_st_clos_tab acct_list;

  g_index NUMBER;

  --------------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync ca,sa account info
  -- ManhNV
  PROCEDURE proc_ddmast_sync IS
    /*v_limit_min NUMBER;
    v_limit_max NUMBER;
    v_limit_count NUMBER;
    v_status      CHAR(1);*/

  BEGIN
    v_start_date      := SYSDATE;
    g_loop_count      := 10;
    g_limit_count     := 200000;
    g_min_count       := 0;
    g_max_count       := 0;
    g_cif_count       := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    /*SELECT sc.is_sync INTO v_status FROM sync_checkpoint sc WHERE sc.sync_type='DDMAST'
    AND TRUNC(sc.sync_end_time) = TRUNC(SYSDATE);
    IF (v_status = 'Y') THEN
        RAISE;
    END IF;

    SELECT sc.sync_count INTO v_limit_count FROM sync_checkpoint sc WHERE sc.sync_type='CFMAST';
    SELECT sc.sync_count INTO v_limit_min FROM sync_checkpoint sc WHERE sc.sync_type='DDMAST';
    --v_limit_min := v_limit_count;
    v_limit_max := v_limit_min + g_limit_count;*/

    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_ddmast
              (bankno,
               branch,
               acctno,
               actype,
               ddctyp,
               cifno,
               status,
               datop7,
               hold,
               cbal,
               odlimt,
               whhirt,
               sccode,
               acname,
               product_type,
               rate,
               accrue)
              (SELECT /*+ ALL_ROWS */
                a.bankno,
                a.branch,
                a.acctno,
                a.actype,
                a.ddctyp,
                a.cifno,
                a.status,
                a.datop7,
                a.hold,
                a.cbal,
                a.odlimt,
                a.whhirt,
                a.sccode,
                a.acname,
                a.sccode,
                a.rate,
                a.accrue
               FROM   RAWSTAGE.SI_DAT_DDMAST@RAWSTAGE_PRO_CORE a
               --WHERE  a.status <> 2 --svdatpv51.DDMAST@DBLINK_DATA a
               WHERE  /*a.ddctyp = 'VND'
                                                                                 AND*/
                a.sccode <> 'CA12OPI'
                                AND a.acctno <> 0


            AND    a.cifno >= g_min_count
                        AND a.cifno < g_max_count);

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    --COMMIT;
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cifno)
    INTO   g_cif_count
    FROM   sync_ddmast a;

    SELECT MIN(a.cifno)
    INTO   g_min_count
    FROM   sync_ddmast a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

           --noformat start
           g_error_level := 5;

           LOOP
               BEGIN
                   SAVEPOINT s_merge_bulk;

                   g_max_count := g_min_count + g_limit_count;

                   /*SELECT \*+ ALL_ROWS *\
                    a.cifno BULK COLLECT
                   INTO   cif_tab
                   FROM   sync_ddmast a
                   WHERE  a.cifno BETWEEN g_min_count AND g_max_count;*/

                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
                   MERGE INTO   bk_account_info c
                        USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
                                       a  .bankno,
                                          a.branch,
                                          (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end) acctno,
                                          TRIM (a.actype) actype,
                                          TRIM (a.ddctyp) ddctyp,
                                          a.cifno,
                                          DECODE (a.status,
                                                  '1', 'ACTV',
                                                  --ACTIVE
                                                  '2', 'CLOS',
                                                  --CLOSED
                                                  '3', 'MATU',
                                                  --MATURED
                                                  '4', 'ACTV',
                                                  --New Record
                                                  '5', 'ACTZ',
                                                  --Active zero balance
                                                  '6', 'REST',
                                                  --RESTRICTED
                                                  '7', 'NOPO',
                                                  --NO POST
                                                  '8', 'COUN',
                                                  --Code unavailable
                                                  '9', 'DORM',
                                                  --DORMANT
                                                  '')
                                              status,
                                          a.datop7,
                                          a.hold,
                                          a.cbal,
                                          a.odlimt,
                                          a.whhirt,
                                          TRIM (a.sccode) sccode,
                                          TRIM (a.acname) acname,
                                          TRIM (a.product_type) product_type,
                                          a.rate,
                                          a.accrue
                                   FROM   sync_ddmast a
                                  WHERE   a.acctno <> 0
                                          -- add STK 13
                                          and  SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '13'
                                          and  SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '31'
                                          AND TRIM (a.status) IS NOT NULL
                                          AND a.cifno >= g_min_count
                                          AND a.cifno < g_max_count --AND    a.cifno = cif_tab(i)
                       --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                                                           --AND    a.status <> 2
                                 --AND    a.product_type <> 'CA12OPI' --tk ki quy
               --AND    SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '31' --tk ki quy
    /* WHERE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       EXISTS (SELECT /*+ INDEX(bk_cif, SYS_C0020268) */
    /*                b.cif_no
    FROM
    bk_cif b
    WHERE
    b.cif_no = a.cifno) */
                                ) src
                           ON   (src.acctno = c.acct_no)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.org_no = (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                           c.branch_no = (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                           c.currency_code = src.ddctyp,
                           c.bank_no = '302',       --a.bankno, core: 27, ibs:302
                           c.status = src.status,
                           c.hold_amount = src.hold,
                           c.ledger_balance = src.cbal,
                           c.available_balance = src.cbal - src.hold + src.odlimt,
                           c.overdraft_limit = src.odlimt,
                           --c.interest_rate     = src.whhirt,
                           c.acct_name = trim(src.acname),
                           c.product_type = src.product_type,
                           c.acct_type = DECODE (src.actype, 'S', 'SA', 'CA'), --s:sa, d:ca
                           c.issued_date = TO_DATE (src.datop7, 'yyyyddd'),
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
                           VALUES   ((case when length(src.acctno) = 13 then LPAD (src.acctno, 14, '0') else TO_CHAR(src.acctno) end),
                                     trim(src.acname),
                                     NULL,
                                     '302',
                                     --a.bankno,
                                     (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                                     --a.branch,
                                     src.product_type,
                                     (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                                     NULL,
                                     DECODE (src.actype, 'S', 'SA', 'CA'),
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
                                     TO_DATE (src.datop7, 'yyyyddd'),
                                     src.rate,
                                     src.accrue);

                   COMMIT;

                   --2011/08/01, Update bc_related_account
                   /*g_error_level := 5;*/

                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
                   /*MERGE INTO bc_related_account c
                   USING (SELECT --\*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) *\
                           LPAD(a.acctno,
                                14,
                                '0') acctno,
                           DECODE(a.status,
                                   '1',
                                   'ACTV',
                                   --ACTIVE
                                   '2',
                                   'CLOS',
                                   --CLOSED
                                   '3',
                                   'MATU',
                                   --MATURED
                                   '4',
                                   'ACTV',
                                   --New Record
                                   '5',
                                   'ACTZ',
                                   --Active zero balance
                                   '6',
                                   'REST',
                                   --RESTRICTED
                                   '7',
                                   'NOPO',
                                   --NO POST
                                   '8',
                                   'COUN',
                                   --Code unavailable
                                   '9',
                                   'DORM',
                                   --DORMANT
                                   '') status,
                           TRIM(a.actype) actype,
                           b.user_id
                           \*ADVICE(337): In Oracle 8 strings of zero length assigned to CHAR
                           variables will blank-pad these rather than making them NULL [111] *\
                          FROM   sync_ddmast a, bc_user_info b
                          WHERE  to_char(a.cifno) = b.cif_no
                          AND    b.status = 'ACTV'
                          AND    a.acctno <> 0
                          AND    TRIM(a.status) IS NOT NULL
                          AND    a.cifno BETWEEN g_min_count AND g_max_count
                          --AND    a.cifno = cif_tab(i)
                          --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                          --AND    a.status <> 2 \* WHERE
                          --EXISTS (SELECT \*+ INDEX(bk_cif, SYS_C0020268) *\
                          \*                b.cif_no
                          FROM
                          bk_cif b
                          WHERE
                          b.cif_no = a.cifno) *\
                          ) src
                   ON (src.acctno = c.acct_no AND c.user_id = src.user_id)
                   WHEN MATCHED THEN
                     UPDATE
                     SET    c.status = src.status
                   WHEN NOT MATCHED THEN
                     INSERT
                     (
                        c.relation_id,
                        c.user_id,
                        c.acct_no,
                        c.acct_type,
                        --c.sub_acct_type,
                        --c.alias,
                        c.is_master,
                        c.status,
                        c.create_time
                     )
                     VALUES
                     (
                        seq_relation_id.nextval,
                        src.user_id,
                        src.acctno,
                        DECODE(src.actype,
                                   'S',
                                   'SA',
                                   'CA'),
                        'N',
                        src.status,
                        SYSDATE
                     );

                     COMMIT;*/

                   g_min_count := g_max_count;

                   --khong co them ban ghi nao
                   EXIT WHEN (g_max_count > g_cif_count);
               EXCEPTION
                   WHEN OTHERS
                   THEN
                       ROLLBACK TO s_merge_bulk;
                       g_error_count := g_error_count + 1;

                       IF (g_error_count >= 10)
                       THEN
                           RAISE;
                       END IF;
               END;
           END LOOP;

           --noformat end

    g_error_level := 6;

    SELECT bai.acct_no BULK COLLECT
    INTO   acct_bka_st_clos_tab
    FROM   bk_account_info bai
    WHERE  bai.status = 'CLOS'
    AND    (acct_type = 'SA' OR acct_type = 'CA');

    g_error_level := 7;

    FORALL i IN acct_bka_st_clos_tab.FIRST .. acct_bka_st_clos_tab.LAST
      DELETE FROM bk_account_info
      WHERE  acct_no = acct_bka_st_clos_tab(i);

    COMMIT;

    g_error_level := 8;

--    SELECT bra.acct_no BULK COLLECT
--    INTO   acct_bkr_st_clos_tab
--    FROM   bc_related_account bra
--    WHERE  bra.status = 'CLOS';

    g_error_level := 9;

--    FORALL i IN acct_bkr_st_clos_tab.FIRST .. acct_bkr_st_clos_tab.LAST
--      DELETE FROM bc_related_account a
--      WHERE  a.acct_no = acct_bkr_st_clos_tab(i);
--
--    COMMIT;

    /*    IF (v_limit_max > v_limit_count) THEN
        UPDATE sync_checkpoint a
        SET
        a.is_sync = 'Y',
        a.sync_count = 0,
        a.sync_end_time = TRUNC(SYSDATE)
        WHERE a.sync_type = 'DDMAST';
        ELSE
            UPDATE sync_checkpoint a
            SET
            a.is_sync = 'N',
            a.sync_count = v_limit_max,
          a.sync_end_time = TRUNC(SYSDATE)
        WHERE a.sync_type = 'DDMAST';
    END IF;*/

    --COMMIT;
    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_ACCOUNT_DDMAST');
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddmast_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_ACCOUNT_DDMAST');
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddmast_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;


  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync fd account info
  -- ManhNV

  PROCEDURE proc_cdmast_sync IS
    /*v_limit_min NUMBER;
    v_limit_max NUMBER;
    v_limit_count NUMBER;
    v_status      CHAR(1);*/
    v_checkpoint_date NUMBER;
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count     := 0;
    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    /*SELECT sc.is_sync INTO v_status FROM sync_checkpoint sc WHERE sc.sync_type='CDMAST'
    AND TRUNC(sc.sync_end_time) = TRUNC(SYSDATE);
    IF (v_status = 'Y') THEN
        RAISE;
    END IF;

    SELECT sc.sync_count INTO v_limit_count FROM sync_checkpoint sc WHERE sc.sync_type='CFMAST';
    SELECT sc.sync_count INTO v_limit_min FROM sync_checkpoint sc WHERE sc.sync_type='CDMAST';
    --v_limit_min := v_limit_count;
    v_limit_max := v_limit_min + g_limit_count;*/

    v_checkpoint_date := 0;

    SELECT (TO_NUMBER(TO_CHAR(SYSDATE,
                              'yyyy') || TO_CHAR(SYSDATE,
                                                 'ddd')) - 100)
    INTO   v_checkpoint_date
    FROM   dual;

    g_error_level := 1;


    --FOR i IN 1 .. g_loop_count


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_cdmast
              (bankno,
               brn,
               acctno,
               cifno,
               cdterm,
               CDTCOD,
               curtyp,
               orgbal,
               cbal,
               accint,
               penamt,
               hold,
               wdrwh,
               cdnum,
               issdt,
               matdt,
               rnwctr,
               status,
               acname,
               TYPE,
               rate,
               renew,
               dactn)
              (SELECT /* +ALL_ROWS */
                b.bankno,
                b.brn,
                b.acctno,
                b.cifno,
                b.cdterm,
                b.CDTCOD,
                b.curtyp,
                b.orgbal,
                b.cbal,
                b.accint,
                b.penamt,
                b.hold,
                b.wdrwh,
                b.cdnum,
                b.issdt,
                b.matdt,
                b.rnwctr,
                b.status,
                b.acname,
                b.TYPE,
                b.rate,
                b.renew,
                b.dactn
               FROM   STAGING.SI_DAT_CDMAST@STAGING_PRO_CORE b
               WHERE
                ((b.matdt > v_checkpoint_date AND b.status = 2) OR
               -- ((b.stmdt > v_checkpoint_date AND b.status = 2) OR
                      b.status <> 2)
                     --WHERE  b.status <> 2
                     --AND b.curtyp = 'VND'
               AND    b.cifno BETWEEN g_min_count AND g_max_count);

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    --COMMIT;
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    g_error_level := 2;


    /*INSERT INTO sync_cif_n
    (cif_no,
     status)
    SELECT a.cif_no,
           'N'
    FROM   bc_user_info a;*/

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cifno)
    INTO   g_cif_count
    FROM   sync_cdmast a;

    SELECT MIN(a.cifno)
    INTO   g_min_count
    FROM   sync_cdmast a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;


    INSERT INTO sync_cif_n
      SELECT a.cif_no,
             'Y'
      FROM   (
              SELECT cif_no
              FROM   bb_corp_info) a;

    --noformat start
    g_error_level := 5;

    LOOP
        BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cifno BULK COLLECT
            INTO   cif_tab
            FROM   sync_cdmast a
            WHERE  a.cifno BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_receipt_info c
                 USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                                (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                   b.cdterm,
                                   b.CDTCOD,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   b.cdnum,
                                   issdt,
                                   matdt,
                                   DECODE (b.status,
                                           '1', 'ACTV',
                                           --ACTIVE
                                           '2', 'CLOS',
                                           --CLOSED
                                           '3', 'MATU',
                                           --MATURED
                                           '4', 'ACTV',
                                           --New Record
                                           '5', 'ACTZ',
                                           --Active zero balance
                                           '6', 'REST',
                                           --RESTRICTED
                                           '7', 'NOPO',
                                           --NO POST
                                           '8', 'COUN',
                                           --Code unavailable
                                           '9', 'DORM',
                                           --DORMANT
                                           '')
                                       status,
                                   TRIM (b.TYPE) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   b.rate,
                                   b.brn
                            FROM   sync_cdmast b
                           /* WHERE
                           EXISTS (SELECT /*+ INDEX(bk_cif, SYS_C0020268) */
                           /*            d.cif_no
                           FROM
                           bk_cif d
                           WHERE
                           d.cif_no = b.cifno) */
                           WHERE       TRIM (b.TYPE) IS NOT NULL
                                   AND acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                                   AND TRIM (b.cifno) IN
                                              (SELECT   cif_no
                                                 FROM   sync_cif_n)
                                   AND b.cifno BETWEEN g_min_count
                                                   AND  g_max_count --AND    b.cifno = cif_tab(i)
                --AND    b.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                                                    --AND    b.status <> 2
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
                    c.cdtcod=a.cdtcod,
                    c.opening_date = TO_DATE (a.issdt, 'yyyyddd'),
                    c.settlement_date = TO_DATE (a.matdt, 'yyyyddd'),
                    c.is_rollout_interest = TRIM (a.renew),
                    c.interest_receive_account = a.dactn,
                    c.status = a.status,
                    c.account_no = (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end)
            WHEN NOT MATCHED
            THEN
                INSERT              (c.receipt_no,
                                     c.product_code,
                                     c.account_no,
                                     c.principal,
                                     c.interest_rate,
                                     c.interest_amount,
                                     c.term,
                                     c.cdtcod,
                                     c.opening_date,
                                     c.settlement_date,
                                     c.is_rollout_interest,
                                     c.interest_receive_account,
                                     c.status)
                    VALUES   ((case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                              a.product_type,
                              (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),
                              --a.cdnum,
                              a.cbal,
                              a.rate,
                              a.accint,
                              a.cdterm,
                              a.cdtcod,
                              TO_DATE (a.issdt, 'yyyyddd'),
                              TO_DATE (a.matdt, 'yyyyddd'),
                              TRIM (a.renew),
                              a.dactn,
                              a.status);

            COMMIT;

            g_error_level := 6;

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_account_info c
                 USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                                (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                   b.brn,
                                   b.cifno,
                                   b.cdterm,
                                   b.curtyp,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   b.cdnum,
                                   b.penamt,
                                   b.hold,
                                   b.issdt,
                                   b.matdt,
                                   trim(b.acname) acname,
                                   DECODE (b.status,
                                           '1', 'ACTV',
                                           --ACTIVE
                                           '2', 'CLOS',
                                           --CLOSED
                                           '3', 'MATU',
                                           --MATURED
                                           '4', 'ACTV',
                                           --New Record
                                           '5', 'ACTZ',
                                           --Active zero balance
                                           '6', 'REST',
                                           --RESTRICTED
                                           '7', 'NOPO',
                                           --NO POST
                                           '8', 'COUN',
                                           --Code unavailable
                                           '9', 'DORM',
                                           --DORMANT
                                           '')
                                       status,
                                   TRIM (b.TYPE) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   b.rate,
                                   b.wdrwh
                            FROM   sync_cdmast b
                           /* WHERE
                           EXISTS (SELECT /*+ INDEX(bk_cif, SYS_C0020268) */
                           /*            d.cif_no
                           FROM
                           bk_cif d
                           WHERE
                           d.cif_no = b.cifno) */
                           WHERE       TRIM (b.TYPE) IS NOT NULL
                                   AND acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                                   AND TRIM (b.cifno) IN
                                              (SELECT   cif_no
                                                 FROM   sync_cif_n)
                                   AND b.cifno BETWEEN g_min_count
                                                   AND  g_max_count --AND    b.cifno = cif_tab(i)
                                                                   --AND    b.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                                                                   --AND    b.status <> 2
                         ) a
                    ON   (a.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET c.bank_no = '302',
                           c.org_no = (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                           c.cif_no = a.cifno,
                           c.acct_type = 'FD',
                           --c.term              = TRIM(a.cdterm),
                           c.currency_code = TRIM (a.curtyp),
                           c.original_balance = a.orgbal,
                           c.principal_balance = a.cbal,
                           c.accured_interest = a.accint,
                           /*c.current_cash_value =
                           (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),*/
                           c.p_acct_no = (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),
                           c.penalty_amount = a.penamt,
                           c.hold_amount = a.hold,
                           c.issued_date = TO_DATE (a.issdt, 'yyyyddd'),
                           c.maturity_date = TO_DATE (a.matdt, 'yyyyddd'),
                           c.acct_name = trim(a.acname),
                           c.product_type = TRIM (a.product_type),
                           c.interest_rate = TRIM (a.rate),
                           c.status = a.status
            WHEN NOT MATCHED
            THEN
                INSERT              (c.acct_no,
                                     c.bank_no,
                                     c.org_no,
                                     c.branch_no,
                                     c.cif_no,
                                     c.acct_type,
                                     --c.term,
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
                                     c.status /*,
                                                                                                               c.current_cash_value*/
                                             )
                    VALUES   ((case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                              '302',
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                              a.cifno,
                              'FD',
                              --TRIM(a.cdterm),
                              TRIM (a.curtyp),
                              a.orgbal,
                              a.cbal,
                              a.accint,
                              a.cdnum,
                              a.penamt,
                              a.hold,
                              TO_DATE (a.issdt, 'yyyyddd'),
                              TO_DATE (a.matdt, 'yyyyddd'),
                              trim(a.acname),
                              a.product_type,
                              a.rate,
                              a.status /*,(a.cbal + a.accint - a.penamt - a.hold - a.wdrwh)*/
                                      );

            COMMIT;

            g_error_level := 7;


            --2011/08/01, Update bc_related_account
            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            /*MERGE INTO bc_related_account c
            USING (SELECT --\*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) *\
                    LPAD(a.acctno,
                         14,
                         '0') acctno,
                    DECODE(a.status,
                            '1',
                            'ACTV',
                            --ACTIVE
                            '2',
                            'CLOS',
                            --CLOSED
                            '3',
                            'MATU',
                            --MATURED
                            '4',
                            'ACTV',
                            --New Record
                            '5',
                            'ACTZ',
                            --Active zero balance
                            '6',
                            'REST',
                            --RESTRICTED
                            '7',
                            'NOPO',
                            --NO POST
                            '8',
                            'COUN',
                            --Code unavailable
                            '9',
                            'DORM',
                            --DORMANT
                            '') status,
                    b.user_id
                   FROM   sync_cdmast a, bc_user_info b
                   WHERE  to_char(a.cifno) = b.cif_no
                   AND    b.status = 'ACTV'
                   AND    a.acctno <> 0
                   AND    TRIM(a.status) IS NOT NULL
                   \*AND    TRIM(a.cifno) IN (SELECT cif_no
                                            FROM   bc_user_info)*\
                   AND    a.cifno BETWEEN g_min_count AND g_max_count
                   --AND    a.cifno = cif_tab(i)
                   --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                   \*AND    a.status <> 2  WHERE
                   EXISTS (SELECT \*+ INDEX(bk_cif, SYS_C0020268) *\
                   \*                b.cif_no
                   FROM
                   bk_cif b
                   WHERE
                   b.cif_no = a.cifno) *\
                   ) src
            ON (src.acctno = c.acct_no AND c.user_id = src.user_id)
            WHEN MATCHED THEN
              UPDATE
              SET    c.status = src.status
            WHEN NOT MATCHED THEN
              INSERT
              (
                 c.relation_id,
                 c.user_id,
                 c.acct_no,
                 c.acct_type,
                 --c.sub_acct_type,
                 --c.alias,
                 c.is_master,
                 c.status,
                 c.create_time
              )
              VALUES
              (
                 seq_relation_id.nextval,
                 src.user_id,
                 src.acctno,
                 'FD',
                 'N',
                 src.status,
                 SYSDATE
              );

            COMMIT;*/

            g_min_count := g_max_count;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_count > g_cif_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    /* when not matched then
    insert
    (
    c.relation_id,
    c.user_id
    )
    values
    (

    );  */
    --AND    a.acct_type = 'FD';
    /*UPDATE bk_account_info a
    SET    a.status = 'CLOS'
    WHERE  (a.current_cash_value = 0 OR a.current_cash_value IS NULL)
    AND    a.acct_type = 'FD'
    AND    a.p_acct_no IS NOT NULL;*/
    g_error_level := 8;


--    SELECT acct_no BULK COLLECT
--    INTO   acct_bcr_st_clos_tab
--    FROM   bc_related_account a
--    WHERE  a.status = 'CLOS';

    g_error_level := 9;

--    FORALL i IN acct_bcr_st_clos_tab.FIRST .. acct_bcr_st_clos_tab.LAST
--      DELETE FROM bc_related_account a
--      WHERE  a.acct_no = acct_bcr_st_clos_tab(i);
--
--    COMMIT;

    g_error_level := 10;

    SELECT receipt_no BULK COLLECT
    INTO   acct_bkr_st_clos_tab
    FROM   bk_receipt_info a
    WHERE  a.status = 'CLOS';

    g_error_level := 11;

    --tam thoi delete cac tai khoan da tat toan
    FORALL i IN acct_bkr_st_clos_tab.FIRST .. acct_bkr_st_clos_tab.LAST
      DELETE FROM bk_receipt_info a
      WHERE  a.receipt_no = acct_bkr_st_clos_tab(i);

    COMMIT;

    g_error_level := 12;

    SELECT acct_no BULK COLLECT
    INTO   acct_bka_st_clos_tab
    FROM   bk_account_info a
    WHERE  a.status = 'CLOS'
    AND    a.acct_type = 'FD';

    g_error_level := 13;

    FORALL i IN acct_bka_st_clos_tab.FIRST .. acct_bka_st_clos_tab.LAST
      DELETE FROM bk_account_info a
      WHERE  a.acct_no = acct_bka_st_clos_tab(i);

    COMMIT;

    /*IF (v_limit_max > v_limit_count) THEN
        UPDATE sync_checkpoint a
        SET
        a.is_sync = 'Y',
        a.sync_count = 0,
        a.sync_end_time = TRUNC(SYSDATE)
        WHERE a.sync_type = 'CDMAST';
        ELSE
            UPDATE sync_checkpoint a
            SET
            a.is_sync = 'N',
            a.sync_count = v_limit_max,
          a.sync_end_time = TRUNC(SYSDATE)
        WHERE a.sync_type = 'CDMAST';
    END IF;*/

    COMMIT;
    --dbms_output.put_line('sysdate: ' || TO_CHAR(SYSDATE, 'dd/mm/yyyy HH24:MI:SS'));
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_ACCOUNT_CDMAST');
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdmast_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_ACCOUNT_CDMAST');
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdmast_sync',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;


  PROCEDURE proc_cdgroup_sync IS
    /*v_limit_min NUMBER;
    v_limit_max NUMBER;
    v_limit_count NUMBER;
    v_status      CHAR(1);*/
  BEGIN
    v_start_date := SYSDATE;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count     := 0;
    g_loop_count      := 10;
    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    /*SELECT sc.is_sync INTO v_status FROM sync_checkpoint sc WHERE sc.sync_type='CDGROUP'
    AND TRUNC(sc.sync_end_time) = TRUNC(SYSDATE);
    IF (v_status = 'Y') THEN
      RAISE;
    END IF;

    SELECT sc.sync_count INTO v_limit_count FROM sync_checkpoint sc WHERE sc.sync_type='CFMAST';
    SELECT sc.sync_count INTO v_limit_min FROM sync_checkpoint sc WHERE sc.sync_type='CDGROUP';
    --v_limit_min := v_limit_count;
    v_limit_max := v_limit_min + g_limit_count;*/

    g_error_level := 1;


    /*INSERT INTO sync_cif_n
    (cif_no,
     status)
    SELECT a.cif_no,
           'N'
    FROM   bc_user_info a;*/

    g_error_level := 2;


    --FOR i IN 1 .. g_loop_count


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_cfagrp a
              (a.cfcifn, --cif
               a.cfagno, --acctno
               --a.CFAPCD,--  Application         code
               --a.CFGPBT,--  Passbook code
               a.cfgsts, --  CF Group status
               a.cfgnam, --  Group account name
               a.cfgcur, --  Group currency
               --a.CFGRAT,--  Group rate
               a.cfggbl, --  Group balance
               --a.CFGPBL,--  Passbook balance
               --a.CFGINT,--  Accrued interest
               --a.CFGPEN,--  Penalty amount
               --a.CFGHLD,--  Hold amount
               --a.CFGFLT,--  Float amount
               --a.CFGCFT,--  Collection float
               a.cfagty, --  Account             Group Type
               --a.CFAGTC,--  Account             product type
               a.cfagd7 --  Group Date          YYYYDDD
               --a.CFCMD7--  Maturity date       for CDs
               --a.CFABRN--  Branch
               )
              (SELECT b.cfcifn,
                      b.cfagno,
                      b.cfgsts,
                      b.cfgnam,
                      b.cfgcur,
                      b.cfggbl,
                      b.cfagty,
                      b.cfagd7
               FROM   RAWSTAGE.SI_DAT_CFAGRP@RAWSTAGE_PRO_CORE b
               WHERE  /*b.cfgcur = 'VND' --AND    b.cfapcd = 'CD' -- for FD
                                                                                                  --AND    TRIM(b.cfgsts) = 'N'
                                                                                 AND*/
                TRIM(b.cfgcur) IS NOT NULL
               --AND    b.cfgcur = 'VND'
               /*AND    TRIM(b.cfcifn) IN (SELECT cif_no
               FROM   sync_cif_n)*/
            AND    b.cfcifn BETWEEN g_min_count AND g_max_count);

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    --COMMIT;
    /*        EXIT;
        \*ADVICE(1154): An EXIT statement is used in a FOR loop [501] *\
      EXCEPTION
        WHEN OTHERS
        \*ADVICE(1157): A WHEN OTHERS clause is used in the exception section
                          without any other specific handlers [201] *\
         THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;*/

    --COMMIT;
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cfcifn)
    INTO   g_cif_count
    FROM   sync_cfagrp a;

    SELECT MIN(a.cfcifn)
    INTO   g_min_count
    FROM   sync_cfagrp a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    INSERT INTO sync_cif_n
      SELECT a.cif_no,
             'Y'
      FROM   (
              SELECT cif_no
              FROM   bb_corp_info) a;

    --noformat start
    g_error_level := 5;

    LOOP
        BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfcifn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfagrp a
            WHERE  a.cfcifn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_account_info c
                 USING   (SELECT   a.cfcifn,
                                (case when length(a.cfagno) = 13 then LPAD (a.cfagno, 14, '0') else TO_CHAR(a.cfagno) end) cfagno,
                                   a.cfgsts,
                                   trim(a.cfgnam) cfgnam,
                                   a.cfgcur,
                                   a.cfggbl,
                                   a.cfagty,
                                   a.cfagd7
                            FROM   sync_cfagrp a
                           WHERE   TRIM (a.cfcifn) IN
                                           (SELECT   cif_no
                                              FROM   sync_cif_n)
                                   AND a.cfcifn BETWEEN g_min_count
                                                    AND  g_max_count --AND    a.cfcifn = cif_tab(i)
                                                                    --WHERE    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                         )
                         src
                    ON   (src.cfagno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.acct_name = trim(src.cfgnam),
                    c.currency_code = TRIM (src.cfgcur),
                    c.issued_date =
                        DECODE (LENGTH (src.cfagd7),
                                7, TO_DATE (src.cfagd7, 'yyyyddd'),
                                NULL),
                    c.status =
                        DECODE (TRIM (src.cfgsts),
                                'N', 'ACTV',
                                'C', 'ACTV',
                                'CLOS'),
                    c.bank_no = '302',                          --tam thoi
                    c.branch_no = SUBSTR ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end), 0, 3)
            WHEN NOT MATCHED
            THEN
                INSERT              (c.acct_no,
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
                    VALUES   ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end),
                              trim(src.cfgnam),
                              src.cfcifn,
                              'FD',
                              TRIM (src.cfgcur),
                              SUBSTR ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end), 0, 3),
                              --tam thoi
                              SUBSTR ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end), 0, 3),
                              DECODE (LENGTH (src.cfagd7),
                                      7, TO_DATE (src.cfagd7, 'yyyyddd'),
                                      NULL),
                              DECODE (LENGTH (src.cfagd7),
                                      7, TO_DATE (src.cfagd7, 'yyyyddd'),
                                      NULL),
                              DECODE (TRIM (src.cfgsts),
                                      'N', 'ACTV',
                                      'C', 'ACTV',
                                      'CLOS'),
                              --'ACTV',
                              SYSDATE,
                              '302');

            COMMIT;

            g_min_count := g_max_count;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_count > g_cif_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    /*IF (v_limit_max > v_limit_count) THEN
      UPDATE sync_checkpoint a
      SET
      a.is_sync = 'Y',
      a.sync_count = 0,
      a.sync_end_time = TRUNC(SYSDATE)
      WHERE a.sync_type = 'CDGROUP';
      ELSE
        UPDATE sync_checkpoint a
        SET
        a.is_sync = 'N',
        a.sync_count = v_limit_max,
        a.sync_end_time = TRUNC(SYSDATE)
      WHERE a.sync_type = 'CDGROUP';
    END IF;*/

    --COMMIT;
    --dbms_output.put_line('sysdate: ' || TO_CHAR(SYSDATE, 'dd/mm/yyyy HH24:MI:SS'));
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_ACCOUNT_CDGROUP');
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdgroup_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_ACCOUNT_CDGROUP');
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdgroup_sync',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;


  PROCEDURE proc_cdgroup_onday_sync IS
    v_checkpoint_date NUMBER;
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    SELECT TO_NUMBER(TO_CHAR(SYSDATE,
                             'yyyy') || TO_CHAR(SYSDATE,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    g_cif_count := g_cif_count + 1000;

    g_error_level := 1;


    --savepoint s_insert_cif;
    --<<s_insert_cif>>
    /*INSERT INTO sync_cif_n
    (cif_no,
     status)
    SELECT a.cif_no,
           'N'
    FROM   bc_user_info a;*/

    g_error_level := 2;


    --<<s_insert_temp>>
    --FOR i IN 1 .. g_loop_count


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        /*LOOP
        BEGIN
          SAVEPOINT s_intert_temp_l;

          g_min_count := g_max_count;
          g_max_count := g_min_count + g_limit_count;*/

        INSERT INTO sync_cfagrp a
          (a.cfcifn, --cif
           a.cfagno, --acctno
           --a.CFAPCD,--  Application         code
           --a.CFGPBT,--  Passbook code
           a.cfgsts, --  CF Group status
           a.cfgnam, --  Group account name
           a.cfgcur, --  Group currency
           --a.CFGRAT,--  Group rate
           a.cfggbl, --  Group balance
           --a.CFGPBL,--  Passbook balance
           --a.CFGINT,--  Accrued interest
           --a.CFGPEN,--  Penalty amount
           --a.CFGHLD,--  Hold amount
           --a.CFGFLT,--  Float amount
           --a.CFGCFT,--  Collection float
           a.cfagty, --  Account             Group Type
           --a.CFAGTC,--  Account             product type
           a.cfagd7 --  Group Date          YYYYDDD
           --a.CFCMD7--  Maturity date       for CDs
           --a.CFABRN--  Branch
           )
          (SELECT b.cfcifn,
                  b.cfagno,
                  b.cfgsts,
                  b.cfgnam,
                  b.cfgcur,
                  b.cfggbl,
                  b.cfagty,
                  b.cfagd7
           FROM   RAWSTAGE.SI_DAT_CFAGRP@RAWSTAGE_PRO_CORE b
           WHERE  b.cfagd7 >= v_checkpoint_date --AND b.cfgcur = 'VND'
           --AND    TRIM(b.cfgsts) = 'N'
           --AND    b.cfapcd = 'CD' -- for FD
           --AND    TRIM(b.cfgcur) IS NOT NULL
           --AND    b.cfcifn BETWEEN g_min_count AND g_max_count
           );

        COMMIT;
        /*--khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;
              IF (g_error_sub_count > 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;*/
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    --COMMIT;
    /*EXIT;
        \*ADVICE(1439): An EXIT statement is used in a FOR loop [501] *\
      EXCEPTION
        WHEN OTHERS
        \*ADVICE(1442): A WHEN OTHERS clause is used in the exception section
                          without any other specific handlers [201] *\
         THEN
          ROLLBACK TO s_insert_temp;
      END;
    END LOOP;*/

    g_error_level := 3;


    --savepoint s_select_min;
    --<<s_select_min>>
    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   min_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       10000) rnm,
                   ROWID rid
            \*ADVICE(1459): Use of ROWID or UROWID [113] *\
            FROM   sync_cfagrp
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 1
    \*ADVICE(1463): This item has not been declared, or it refers to a label [131] *\
    ORDER  BY 1;*/

    g_error_level := 4;


    --savepoint s_select_max;
    --<<s_select_max>>
    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   max_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       10000) rnm,
                   ROWID rid
            \*ADVICE(1476): Use of ROWID or UROWID [113] *\
            FROM   sync_cfagrp
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 0
    \*ADVICE(1480): This item has not been declared, or it refers to a label [131] *\
    UNION
    SELECT \*+ ALL_ROWS *\
     MAX(ROWID)
    FROM   sync_cfagrp;*/


    /*g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cfcifn)
    INTO   g_cif_count
    FROM   sync_cfagrp a;
    SELECT MIN(a.cfcifn)
    INTO   g_min_count
    FROM   sync_cfagrp a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;*/

    /*SELECT \*+ ALL_ROWS *\
     a.cfcifn BULK COLLECT
    INTO   cif_tab
    FROM   sync_cfagrp a
    WHERE  a.cfcifn BETWEEN g_min_count AND g_max_count;*/

    INSERT INTO sync_cif_n
      SELECT a.cif_no,
             'Y'
      FROM   (
              SELECT cif_no
              FROM   bb_corp_info) a;

    --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
    MERGE INTO bk_account_info c
    USING (SELECT a.cfcifn,
                (case when length(a.cfagno) = 13 then LPAD (a.cfagno, 14, '0') else TO_CHAR(a.cfagno) end) cfagno,
                  a.cfgsts,
                  trim(a.cfgnam) cfgnam,
                  a.cfgcur,
                  a.cfggbl,
                  a.cfagty,
                  a.cfagd7
           FROM   sync_cfagrp a
           WHERE  /*a.cfcifn BETWEEN g_min_count AND g_max_count
                                                                                 AND*/
            TRIM(a.cfgcur) IS NOT NULL
     AND    TRIM(a.cfcifn) IN (SELECT cif_no
                               FROM   sync_cif_n) --AND    a.cfcifn = cif_tab(i)
           --WHERE    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           ) src
    ON (src.cfagno = c.acct_no)
    WHEN MATCHED THEN
      UPDATE
      SET    c.acct_name     = trim(src.cfgnam),
             c.currency_code = TRIM(src.cfgcur),
             c.issued_date   = DECODE(LENGTH(src.cfagd7),
                                      7,
                                      TO_DATE(src.cfagd7,
                                              'yyyyddd'),
                                      NULL),
             c.status        = DECODE(TRIM(src.cfgsts),
                                      'N',
                                      'ACTV',
                                      'C',
                                      'ACTV',
                                      'CLOS'),
             c.bank_no       = '302', --tam thoi
             c.branch_no     = SUBSTR((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end),
                                      0,
                                      3)
    WHEN NOT MATCHED THEN
      INSERT
        (c.acct_no,
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
      VALUES
        ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end) ,
         trim(src.cfgnam),
         src.cfcifn,
         'FD',
         TRIM(src.cfgcur),
         SUBSTR((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end),
                0,
                3), --tam thoi
         SUBSTR((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end) ,
                0,
                3),
         DECODE(LENGTH(src.cfagd7),
                7,
                TO_DATE(src.cfagd7,
                        'yyyyddd'),
                NULL),
         DECODE(LENGTH(src.cfagd7),
                7,
                TO_DATE(src.cfagd7,
                        'yyyyddd'),
                NULL),
         DECODE(TRIM(src.cfgsts),
                'N',
                'ACTV',
                'C',
                'ACTV',
                'CLOS'),
         --'ACTV',
         SYSDATE,
         '302');

    COMMIT;

    /*g_min_count := g_max_count;

        --khong co them ban ghi nao
        EXIT WHEN(g_max_count > g_cif_count);

       EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_merge_bulk;
          g_error_count := g_error_count + 1;
          IF (g_error_count > 10)
          THEN
            RAISE;
          END IF;
    END;
    END LOOP;*/
    --noformat end

    --dbms_output.put_line('sysdate: ' || TO_CHAR(SYSDATE, 'dd/mm/yyyy HH24:MI:SS'));
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdgroup_onday_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      /*g_error_count := g_error_count + 1;
      IF (g_error_count = 10) THEN
         ROLLBACK;
       elsif (g_error_count != 10 AND g_error_level = 1) THEN
         ROLLBACK TO s_insert_cif;
         GOTO s_insert_cif1;
       elsif (g_error_count != 10 AND g_error_level = 2) THEN
         ROLLBACK TO s_insert_temp;
         GOTO s_insert_temp;
       elsif (g_error_count != 10 AND g_error_level = 3) THEN
         ROLLBACK TO s_select_min;
         GOTO s_select_min;
       elsif (g_error_count != 10 AND g_error_level = 4) THEN
         ROLLBACK TO s_select_max;
         GOTO s_select_max;
       elsif (g_error_count != 10 AND g_error_level = 5) THEN
         ROLLBACK TO s_merge_bulk;
         GOTO s_merge_bulk;
       END IF;*/

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdgroup_onday_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;



  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync loan account info
  -- ManhNV

  PROCEDURE proc_lnmast_sync IS
    /*v_limit_min NUMBER;
    v_limit_max NUMBER;
    v_limit_count NUMBER;
    v_status      CHAR(1);*/
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count := 0;

    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    /*SELECT sc.is_sync INTO v_status FROM sync_checkpoint sc WHERE sc.sync_type='LNMAST'
    AND TRUNC(sc.sync_end_time) = TRUNC(SYSDATE);
    IF (v_status = 'Y') THEN
      RAISE;
    END IF;

    SELECT sc.sync_count INTO v_limit_count FROM sync_checkpoint sc WHERE sc.sync_type='CFMAST';
    SELECT sc.sync_count INTO v_limit_min FROM sync_checkpoint sc WHERE sc.sync_type='LNMAST';
    --v_limit_min := v_limit_count;
    v_limit_max := v_limit_min + g_limit_count;*/

    g_error_level := 1;


    /*INSERT INTO sync_cif_n
    (cif_no,
     status)
    SELECT a.cif_no,
           'N'
    FROM   bc_user_info a;*/

    g_error_level := 2;


    --FOR i IN 1 .. g_loop_count
    /*INSERT INTO sync_cif_n
      SELECT a.cif_no,
             'Y'
      FROM   (SELECT cif_no
              FROM   bc_user_info
              UNION
              SELECT cif_no
              FROM   bb_corp_info) a;*/

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_lnmast
              SELECT /*+ ALL_ROWS */
               b       .bkn,
               b.brn,
               b.accint, --LNPDUE.PDIINT,
               b.TYPE   produc_type,
               b.cifno,
               b.lnnum,
               b.acctno,
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
               b.fulldt,
               b.status,
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
               b.acname,
               b.datopn,
               b.matdt,
               b.FRELDT ,
               b.rate,
               trim(b.tmcode)
              FROM   RAWSTAGE.SI_DAT_LNMAST@RAWSTAGE_PRO_CORE b
              --WHERE  b.status <> 2
              WHERE  /*b.curtyp = 'VND'
                                                                             AND*/
               /*TRIM(b.cifno) IN (SELECT cif_no
                                 FROM   sync_cif_n)
               AND*/    b.cifno BETWEEN g_min_count AND g_max_count;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    --COMMIT;
    /*        EXIT;
        \*ADVICE(1769): An EXIT statement is used in a FOR loop [501] *\
      EXCEPTION
        WHEN OTHERS
        \*ADVICE(1772): A WHEN OTHERS clause is used in the exception section
                          without any other specific handlers [201] *\
         THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;*/

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cifno)
    INTO   g_cif_count
    FROM   sync_lnmast a;

    SELECT MIN(a.cifno)
    INTO   g_min_count
    FROM   sync_lnmast a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
    g_error_level := 5;

    LOOP
        BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /* SELECT \*+ ALL_ROWS *\
              a.cifno BULK COLLECT
             INTO   cif_tab
             FROM   sync_lnmast a
             WHERE  a.cifno BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_account_info c
                 USING   (SELECT /*+ INDEX(sync_lnmast, IDX_SYNC_LNMAST) */
                                b  .bkn,
                                   b.brn,
                                   b.accint,              --LNPDUE.PDIINT,
                                   TRIM (b.produc_type) product_type,
                                   b.cifno,
                                   b.lnnum,
                                   (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
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
                                   fulldt,
                                   DECODE (b.status,
                                           '1', 'ACTV',
                                           --ACTIVE
                                           '2', 'CLOS',
                                           --CLOSED
                                           '3', 'MATU',
                                           --MATURED
                                           '4', 'ACTV',
                                           --New Record
                                           '5', 'ACTZ',
                                           --Active zero balance
                                           '6', 'REST',
                                           --RESTRICTED
                                           '7', 'NOPO',
                                           --NO POST
                                           '8', 'COUN',
                                           --Code unavailable
                                           '9', 'DORM',
                                           --DORMANT
                                           'ACTV')
                                       status,
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
                                   datopn,
                                   matdt,
                                   b.FRELDT,
                                   b.rate,
                                   b.tmcode
                            FROM   sync_lnmast b
                           WHERE   b.acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                                   /*AND TRIM (b.cifno) IN
                                              (SELECT   cif_no
                                                 FROM   sync_cif_n
                                               )*/
                                   AND b.cifno BETWEEN g_min_count
                                                   AND  g_max_count --AND    b.cifno = cif_tab(i)
                                                                   --AND    b.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                         ) a
                    ON   (a.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.bank_no = '302',                            --a.bkn,
                    c.org_no = (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),              --a.brn,
                    c.branch_no = (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),           --a.brn,
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
                    c.full_release_date =
                        DECODE (a.fulldt,
                                0, NULL,
                                TO_DATE (a.fulldt, 'yyyyddd')),
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
                    c.issued_date =
                        DECODE (a.datopn,
                                0, NULL,
                                TO_DATE (a.datopn, 'yyyyddd')),
                    c.maturity_date =
                        DECODE (a.matdt,
                                0, NULL,
                                TO_DATE (a.matdt, 'yyyyddd')),
                    c.cif_no = a.cifno,
                    c.available_date = DECODE (a.FRELDT, 0, NULL, TO_DATE (a.FRELDT, 'yyyyddd')) ,
                    c.interest_rate=  a.rate
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
                                     c.remark)
                    VALUES   ('302',                              --a.bkn,
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),               --a.brn,
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),               --a.brn,
                              a.accint,
                              a.cifno,
                              TRIM (a.lnnum),
                              (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                              TRIM (a.purcod),
                              TRIM (a.curtyp),
                              a.orgamt,
                              a.cbal,
                              a.ysobal + a.billco,             /*a.TERM,*/
                              TRIM (a.freq),
                              a.ipfreq,
                              DECODE (a.fulldt,
                                      0, NULL,
                                      TO_DATE (a.fulldt, 'yyyyddd')),
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
                              DECODE (a.datopn,
                                      0, NULL,
                                      TO_DATE (a.datopn, 'yyyyddd')),
                              DECODE (a.matdt,
                                      0, NULL,
                                      TO_DATE (a.matdt, 'yyyyddd')),
                              DECODE (a.FRELDT,
                                      0, NULL,
                                      TO_DATE (a.FRELDT, 'yyyyddd')),
                              a.rate,
                              a.TERM || a.tmcode
                                      );

            COMMIT;

            g_error_level := 6;


            --2011/08/01, Update bc_related_account
            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            /*MERGE INTO bc_related_account c
            USING (SELECT --\*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) *\
                    LPAD(a.acctno,
                         14,
                         '0') acctno,
                    DECODE(a.status,
                           '1',
                           'ACTV',
                           --ACTIVE
                           '2',
                           'CLOS',
                           --CLOSED
                           '3',
                           'MATU',
                           --MATURED
                           '4',
                           'ACTV',
                           --New Record
                           '5',
                           'ACTZ',
                           --Active zero balance
                           '6',
                           'REST',
                           --RESTRICTED
                           '7',
                           'NOPO',
                           --NO POST
                           '8',
                           'COUN',
                           --Code unavailable
                           '9',
                           'DORM',
                           --DORMANT
                           'ACTV') status,
                           b.user_id
                   FROM   sync_lnmast a, bc_user_info b
                   WHERE  trim(a.cifno) = b.cif_no
                   AND    a.acctno <> 0
                   AND    TRIM(a.status) IS NOT NULL
                             \*AND    TRIM(a.cifno) IN (SELECT cif_no
                                                                                 FROM   bc_user_info)*\
                             AND    a.cifno BETWEEN g_min_count AND g_max_count
                             --AND    a.cifno = cif_tab(i)
                             --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                   ) src
            ON (src.acctno = c.acct_no AND c.user_id = src.user_id)
            WHEN MATCHED THEN
              UPDATE
              SET    c.status = src.status;

            COMMIT;*/

            g_min_count := g_max_count;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_count > g_cif_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    g_error_level := 7;

    SELECT acct_no BULK COLLECT
    INTO   acct_bka_st_clos_tab
    FROM   bk_account_info
    WHERE  status = 'CLOS'
    AND    acct_type = 'LN';

    g_error_level := 8;


    FORALL i IN acct_bka_st_clos_tab.FIRST .. acct_bka_st_clos_tab.LAST
      DELETE FROM bk_account_info
      WHERE  acct_no = acct_bka_st_clos_tab(i);

    COMMIT;

    g_error_level := 9;

--    SELECT acct_no BULK COLLECT
--    INTO   acct_bkr_st_clos_tab
--    FROM   bc_related_account
--    WHERE  status = 'CLOS';

    g_error_level := 10;


--    FORALL i IN acct_bkr_st_clos_tab.FIRST .. acct_bkr_st_clos_tab.LAST
--      DELETE FROM bc_related_account a
--      WHERE  acct_no = acct_bkr_st_clos_tab(i);
--
--    COMMIT;

    /*IF (v_limit_max > v_limit_count) THEN
      UPDATE sync_checkpoint a
      SET
      a.is_sync = 'Y',
      a.sync_count = 0,
      a.sync_end_time = TRUNC(SYSDATE)
      WHERE a.sync_type = 'LNMAST';
      ELSE
        UPDATE sync_checkpoint a
        SET
        a.is_sync = 'N',
        a.sync_count = v_limit_max,
        a.sync_end_time = TRUNC(SYSDATE)
      WHERE a.sync_type = 'LNMAST';
    END IF;*/

    --COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_ACCOUNT_LNMAST');
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_lnmast_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_ACCOUNT_LNMAST');
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_lnmast_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;



  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync new ca,sa account info (be created on day)
  -- ManhNV

  PROCEDURE proc_ddtnew_sync IS
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count := 0;

    g_index := 0;

    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    g_error_level := 1;


    --FOR i IN 1 .. g_loop_count


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_ddtnew
              SELECT /*+ ALL_ROWS */
               a.bankno,
               a.branch,
               a.acctno,
               a.actype,
               a.ddctyp,
               a.cifno,
               a.status,
               a.datop7,
               a.hold,
               a.cbal,
               a.odlimt,
               a.whhirt,
               a.acname,
               '',
               a.sccode product_type,
               a.rate,
               a.accrue
              FROM   RAWSTAGE.SI_DAT_DDTNEW@RAWSTAGE_PRO_CORE a
              WHERE  a.status <> 2
              AND    a.acctno <> 0 --AND a.ddctyp = 'VND'
              AND    a.sccode <> 'CA12OPI'

              AND    a.cifno BETWEEN g_min_count AND g_max_count;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    /*        EXIT;
        \*ADVICE(2226): An EXIT statement is used in a FOR loop [501] *\
      EXCEPTION
        WHEN OTHERS
        \*ADVICE(2229): A WHEN OTHERS clause is used in the exception section
                          without any other specific handlers [201] *\
         THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;*/



    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cifno)
    INTO   g_cif_count
    FROM   sync_ddtnew a;

    SELECT MIN(a.cifno)
    INTO   g_min_count
    FROM   sync_ddtnew a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

           --noformat start
           g_error_level := 5;

           LOOP
               BEGIN
                   SAVEPOINT s_merge_bulk;

                   g_max_count := g_min_count + g_limit_count;

                   /*SELECT \*+ ALL_ROWS *\
                    a.cifno BULK COLLECT
                   INTO   cif_tab
                   FROM   sync_ddtnew a
                   WHERE  a.cifno BETWEEN g_min_count AND g_max_count;*/

                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
                   MERGE INTO   bk_account_info c
                        USING   (SELECT /*+ INDEX(sync_ddtnew, IDX_SYNC_DDTNEW) */
                                       a  .bankno,
                                          a.branch,
                                          (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end) acctno,
                                          a.actype,
                                          a.ddctyp,
                                          a.cifno,
                                          DECODE (a.status,
                                                  '1', 'ACTV',
                                                  --ACTIVE
                                                  '2', 'CLOS',
                                                  --CLOSED
                                                  '3', 'MATU',
                                                  --MATURED
                                                  '4', 'ACTV',
                                                  --'NEWR', --New Record
                                                  '5', 'ACTZ',
                                                  --Active zero balance
                                                  '6', 'REST',
                                                  --RESTRICTED
                                                  '7', 'NOPO',
                                                  --NO POST
                                                  '8', 'COUN',
                                                  --Code unavailable
                                                  '9', 'DORM',
                                                  --DORMANT
                                                  'ACTV')
                                              status,
                                          a.datop7,
                                          a.hold,
                                          a.cbal,
                                          a.odlimt,
                                          a.whhirt,
                                          trim(a.acname) acname,
                                          TRIM (a.product_type) product_type,
                                          a.rate,
                                          a.accrue
                                   FROM   sync_ddtnew a
                                  WHERE                           --a.acctno <> 0
                                       TRIM (a.status) IS NOT NULL
                                        and  SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '13'
                                        and  SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '31'
                                          AND a.cifno BETWEEN g_min_count
                                                          AND  g_max_count --AND    a.cifno = cif_tab(i)
    /*AND    a.status <> 2
                                                                                                                           AND    a.product_type <> 'CA12OPI'*/
              --OR     SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '31') --tk ki quy
    /* WHERE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       EXISTS (
    SELECT /*+ INDEX(bk_cif, SYS_C0020268) */
    /*                   d.cif_no
    FROM
    bk_cif d
    WHERE
    d.cif_no = a.cifno
    ) */
                                ) src
                           ON   (src.acctno = c.acct_no)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.status = src.status,
                           c.interest_rate = src.rate,
                           c.accured_interest = src.accrue
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.bank_no,
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
                           VALUES   ('302',
                                     --a.bankno,
                                     (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                                     (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                                     (case when length(src.acctno) = 13 then LPAD (src.acctno, 14, '0') else TO_CHAR(src.acctno) end),
                                     DECODE (TRIM (src.actype), 'S', 'SA', 'CA'),
                                     TRIM (src.ddctyp),
                                     src.cifno,
                                     src.status,
                                     TO_DATE (datop7, 'yyyyddd'),
                                     src.hold,
                                     src.cbal,
                                     src.cbal - src.hold,
                                     src.odlimt,
                                     src.rate,
                                     TRIM (src.acname),
                                     src.product_type,
                                     TO_DATE (datop7, 'yyyyddd'),
                                     src.accrue);

                   COMMIT;


                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
--                   MERGE INTO   bc_related_account c
--                        USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
--                                       LPAD (a.acctno, 14, '0') acctno,
--                                          DECODE (a.status,
--                                                  '1', 'ACTV',
--                                                  --ACTIVE
--                                                  '2', 'CLOS',
--                                                  --CLOSED
--                                                  '3', 'MATU',
--                                                  --MATURED
--                                                  '4', 'ACTV',
--                                                  --New Record
--                                                  '5', 'ACTZ',
--                                                  --Active zero balance
--                                                  '6', 'REST',
--                                                  --RESTRICTED
--                                                  '7', 'NOPO',
--                                                  --NO POST
--                                                  '8', 'COUN',
--                                                  --Code unavailable
--                                                  '9', 'DORM',
--                                                  --DORMANT
--                                                  '')
--                                              status,
--                                          TRIM (a.actype) actype,
--                                          b.user_id
--                                   FROM   sync_ddtnew a, bc_user_info b
--                                  WHERE       TO_CHAR (a.cifno) = b.cif_no
--                                          AND b.status = 'ACTV'
--                                          AND a.acctno <> 0
--                                          AND TRIM (a.status) IS NOT NULL
--                                          AND a.cifno BETWEEN g_min_count
--                                                          AND  g_max_count --AND    a.cifno = cif_tab(i)
--                       --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
--                                                  --AND    a.status <> 2 /* WHERE
--                              --EXISTS (SELECT /*+ INDEX(bk_cif, SYS_C0020268) */
--    /*                b.cif_no
--    FROM
--    bk_cif b
--    WHERE
--    b.cif_no = a.cifno) */
--                                ) src
--                           ON   (src.acctno = c.acct_no
--                                 AND src.user_id = c.user_id)
--                   WHEN MATCHED
--                   THEN
--                       UPDATE SET c.status = src.status
--                   WHEN NOT MATCHED
--                   THEN
--                       INSERT              (c.relation_id,
--                                            c.user_id,
--                                            c.acct_no,
--                                            c.acct_type,
--                                            --c.sub_acct_type,
--                                            --c.alias,
--                                            c.is_master,
--                                            c.status,
--                                            c.create_time)
--                           VALUES   (seq_relation_id.NEXTVAL,
--                                     src.user_id,
--                                     src.acctno,
--                                     DECODE (src.actype, 'S', 'SA', 'CA'),
--                                     'N',
--                                     src.status,
--                                     SYSDATE);
--
--                   COMMIT;

                   g_min_count := g_max_count;

                   --khong co them ban ghi nao
                   EXIT WHEN (g_max_count > g_cif_count);
               EXCEPTION
                   WHEN OTHERS
                   THEN
                       ROLLBACK TO s_merge_bulk;
                       g_error_count := g_error_count + 1;

                       IF (g_error_count >= 10)
                       THEN
                           RAISE;
                       END IF;
               END;
           END LOOP;

           --noformat end

    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddtnew_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddtnew_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
  END;



  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync new fd account info (be created on day)
  -- ManhNV

  PROCEDURE proc_cdtnew_sync IS
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count := 0;

    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    g_error_level := 1;


    --FOR i IN 1 .. g_loop_count


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_cdtnew
              (bankno,
               brn,
               curtyp,
               cifno,
               orgbal,
               cbal,
               accint,
               penamt,
               hold,
               wdrwh,
               cdnum,
               issdt,
               matdt,
               rnwctr,
               status,
               acname,
               acctno,
               product_type,
               rate,
               renew,
               dactn,
               cdterm)  
              SELECT /*+ ALL_ROWS */
               b.bankno,
               b.brn,
               b.curtyp,
               b.cifno,
               b.orgbal,
               b.cbal,
               b.accint,
               b.penamt,
               b.hold,
               b.wdrwh,
               b.cdnum,
               b.issdt,
               b.matdt,
               b.rnwctr,
               b.status,
               b.acname,
               b.acctno,
               b.TYPE   product_type,
               b.rate,
               b.renew,
               b.dactn,
               b.cdterm
              FROM   RAWSTAGE.SI_DAT_CDTNEW@RAWSTAGE_PRO_CORE b
              WHERE  b.status <> 2 --AND b.curtyp = 'VND'
              AND    b.status <> 0
                    --AND    TRIM(b.type) IS NOT NULL;
              AND    b.cifno BETWEEN g_min_count AND g_max_count;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    --COMMIT;
    /*        EXIT;
        \*ADVICE(2527): An EXIT statement is used in a FOR loop [501] *\
      EXCEPTION
        WHEN OTHERS
        \*ADVICE(2530): A WHEN OTHERS clause is used in the exception section
                          without any other specific handlers [201] *\
         THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;*/

    g_error_level := 2;


    /*INSERT INTO sync_cif_n
    (cif_no,
     status)
    SELECT a.cif_no,
           'N'
    FROM   bc_user_info a;*/

    --g_limit_count := 10000;

    SELECT MAX(a.cifno)
    INTO   g_cif_count
    FROM   sync_cdtnew a;

    SELECT MIN(a.cifno)
    INTO   g_min_count
    FROM   sync_cdtnew a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    INSERT INTO sync_cif_n
      SELECT a.cif_no,
             'Y'
      FROM   (
              SELECT cif_no
              FROM   bb_corp_info) a;

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

           --noformat start
           g_error_level := 5;

           LOOP
               BEGIN
                   SAVEPOINT s_merge_bulk;

                   g_max_count := g_min_count + g_limit_count;

                   /*SELECT \*+ ALL_ROWS *\
                    a.cifno BULK COLLECT
                   INTO   cif_tab
                   FROM   sync_cdtnew a
                   WHERE  a.cifno BETWEEN g_min_count AND g_max_count;*/

                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
                   MERGE INTO   bk_receipt_info c
                        USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                        (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                          b.cdterm,
                                          b.orgbal,
                                          b.accint,
                                          b.cdnum,
                                          issdt,
                                          matdt,
                                          DECODE (b.status,
                                                  '1', 'ACTV',
                                                  --ACTIVE
                                                  '2', 'CLOS',
                                                  --CLOSED
                                                  '3', 'MATU',
                                                  --MATURED
                                                  '4', 'ACTV',
                                                  --'NEWR', --New Record
                                                  '5', 'ACTZ',
                                                  --Active zero balance
                                                  '6', 'REST',
                                                  --RESTRICTED
                                                  '7', 'NOPO',
                                                  --NO POST
                                                  '8', 'COUN',
                                                  --Code unavailable
                                                  '9', 'DORM',
                                                  --DORMANT
                                                  '')
                                              status,
                                          TRIM (b.product_type) product_type,
                                          TRIM (b.renew) renew,
                                          b.dactn,
                                          b.rate,
                                          b.cbal
                                   FROM   sync_cdtnew b
                                  WHERE
                                                              b.cifno BETWEEN g_min_count
                                                          AND  g_max_count
                                                             AND TRIM (b.status) IS NOT NULL
                                          AND TRIM (b.cifno) IN
                                                     (SELECT   cif_no
                                                        FROM   sync_cif_n
                                                      )
                                          AND TRIM (b.product_type) IS NOT NULL
                                           --AND    b.cifno = cif_tab(i)
                                                                          --and    b.rowid between min_rid_tab(i) and max_rid_tab(i)
                                ) a
                           ON   (a.acctno = c.receipt_no)
                   /*WHEN MATCHED THEN
             UPDATE
             SET    c.product_code             = a.product_type,
                    c.principal                = a.cbal,
                    c.interest_rate            = a.rate,
                    c.interest_amount          = a.accint,
                    c.term                     = a.cdterm,
                    c.opening_date             = TO_DATE(a.issdt,
                                                         'yyyyddd'),
                    c.settlement_date          = TO_DATE(a.matdt,
                                                         'yyyyddd'), --cai nay xem lai
                    c.is_rollout_interest      = a.renew,
                    c.interest_receive_account = a.dactn,
                    c.status                   = a.status,
                    c.account_no               = LPAD(a.cdnum,
                                                      14,
                                                      '0')*/
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
                           VALUES   ( (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                                     a.product_type,
                                     (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),          --a.cdnum,
                                     a.cbal,
                                     a.rate,
                                     a.accint,
                                     a.cdterm,
                                     TO_DATE (a.issdt, 'yyyyddd'),
                                     TO_DATE (a.matdt, 'yyyyddd'),
                                     TRIM (a.renew),
                                     a.dactn,
                                     a.status);

                   COMMIT;

                   --SAVEPOINT s_merge_bulk;

                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
                   MERGE INTO   bk_account_info c
                        USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                                        (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                          b.brn,
                                          b.cifno,
                                          b.cdterm,
                                          b.curtyp,
                                          b.cbal,
                                          b.orgbal,
                                          b.accint,
                                          b.cdnum,
                                          b.penamt,
                                          b.hold,
                                          b.issdt,
                                          b.matdt,
                                          trim(b.acname) acname,
                                          DECODE (b.status,
                                                  '1', 'ACTV',
                                                  --ACTIVE
                                                  '2', 'CLOS',
                                                  --CLOSED
                                                  '3', 'MATU',
                                                  --MATURED
                                                  '4', 'ACTV',
                                                  --New Record
                                                  '5', 'ACTZ',
                                                  --Active zero balance
                                                  '6', 'REST',
                                                  --RESTRICTED
                                                  '7', 'NOPO',
                                                  --NO POST
                                                  '8', 'COUN',
                                                  --Code unavailable
                                                  '9', 'DORM',
                                                  --DORMANT
                                                  '')
                                              status,
                                          TRIM (b.product_type) product_type,
                                          TRIM (b.renew) renew,
                                          b.dactn,
                                          b.rate,
                                          b.wdrwh
                                   FROM   sync_cdtnew b
                                  WHERE   b.cifno BETWEEN g_min_count
                                                          AND  g_max_count
                                                                     AND TRIM (b.product_type) IS NOT NULL
                                          AND acctno <> 0
                                          AND TRIM (b.status) IS NOT NULL
                                          AND TRIM (b.cifno) IN
                                                     (SELECT   cif_no
                                                        FROM   sync_cif_n
                                                      )
                                           --AND    b.cifno = cif_tab(i)
                                                                          --AND    b.ROWID between min_rid_tab(i) and max_rid_tab(i)
                                                                          --AND    b.status <> 2
                                ) a
                           ON   (a.acctno = c.acct_no)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.bank_no = '302',
                           c.org_no = (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                           c.cif_no = a.cifno,
                           c.acct_type = 'FD',
                           --c.term              = TRIM(a.cdterm),
                           c.currency_code = TRIM (a.curtyp),
                           c.original_balance = a.orgbal,
                           c.principal_balance = a.cbal,
                           c.accured_interest = a.accint,
                           c.p_acct_no = (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),
                           c.penalty_amount = a.penamt,
                           c.current_cash_value =
                               (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
                           c.hold_amount = a.hold,
                           c.issued_date = TO_DATE (a.issdt, 'yyyyddd'),
                           c.maturity_date = TO_DATE (a.matdt, 'yyyyddd'),
                           c.acct_name = trim(a.acname),
                           c.product_type = TRIM (a.product_type),
                           c.interest_rate = TRIM (a.rate),
                           c.status = a.status
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.acct_no,
                                            c.bank_no,
                                            c.org_no,
                                            c.branch_no,
                                            c.cif_no,
                                            c.acct_type,
                                            --c.term,
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
                           VALUES   ((case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                                     '302',
                                     (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                                     (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                                     a.cifno,
                                     'FD',
                                     --TRIM(a.cdterm),
                                     TRIM (a.curtyp),
                                     a.orgbal,
                                     a.cbal,
                                     a.accint,
                                     a.cdnum,
                                     a.penamt,
                                     a.hold,
                                     TO_DATE (a.issdt, 'yyyyddd'),
                                     TO_DATE (a.matdt, 'yyyyddd'),
                                     trim(a.acname),
                                     a.product_type,
                                     a.rate,
                                     a.status,
                                     (  a.cbal
                                      + a.accint
                                      - a.penamt
                                      - a.hold
                                      - a.wdrwh));

                   COMMIT;

                   --SAVEPOINT s_merge_bulk;

                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
--                   MERGE INTO   bc_related_account c
--                        USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
--                                       LPAD (a.acctno, 14, '0') acctno,
--                                          DECODE (a.status,
--                                                  '1', 'ACTV',
--                                                  --ACTIVE
--                                                  '2', 'CLOS',
--                                                  --CLOSED
--                                                  '3', 'MATU',
--                                                  --MATURED
--                                                  '4', 'ACTV',
--                                                  --New Record
--                                                  '5', 'ACTZ',
--                                                  --Active zero balance
--                                                  '6', 'REST',
--                                                  --RESTRICTED
--                                                  '7', 'NOPO',
--                                                  --NO POST
--                                                  '8', 'COUN',
--                                                  --Code unavailable
--                                                  '9', 'DORM',
--                                                  --DORMANT
--                                                  '')
--                                              status,
--                                          b.user_id
--                                   FROM   sync_cdtnew a, bc_user_info b
--                                  WHERE       TO_CHAR (a.cifno) = b.cif_no
--                                          AND b.status = 'ACTV'
--                                          AND a.acctno <> 0
--                                          AND TRIM (a.status) IS NOT NULL
--                                          /*AND    TRIM(a.cifno) IN (SELECT cif_no
--                                                                   FROM   bc_user_info)*/
--                                          AND a.cifno BETWEEN g_min_count
--                                                          AND  g_max_count --AND    a.cifno = cif_tab(i)
--                       --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
--    /*AND    a.status <> 2  WHERE
--    EXISTS (SELECT /*+ INDEX(bk_cif, SYS_C0020268) */
--    /*                b.cif_no
--    FROM
--    bk_cif b
--    WHERE
--    b.cif_no = a.cifno) */
--                                ) src
--                           ON   (src.acctno = c.acct_no
--                                 AND src.user_id = c.user_id)
--                   WHEN MATCHED
--                   THEN
--                       UPDATE SET c.status = src.status
--                   WHEN NOT MATCHED
--                   THEN
--                       INSERT              (c.relation_id,
--                                            c.user_id,
--                                            c.acct_no,
--                                            c.acct_type,
--                                            --c.sub_acct_type,
--                                            --c.alias,
--                                            c.is_master,
--                                            c.status,
--                                            c.create_time)
--                           VALUES   (seq_relation_id.NEXTVAL,
--                                     src.user_id,
--                                     src.acctno,
--                                     'FD',
--                                     'N',
--                                     src.status,
--                                     SYSDATE);
--
--                   COMMIT;

                   g_min_count := g_max_count;

                   --khong co them ban ghi nao
                   EXIT WHEN (g_max_count > g_cif_count);
               EXCEPTION
                   WHEN OTHERS
                   THEN
                       ROLLBACK TO s_merge_bulk;
                       g_error_count := g_error_count + 1;

                       IF (g_error_count >= 10)
                       THEN
                           RAISE;
                       END IF;
               END;
           END LOOP;

           --noformat end
    g_error_level := 6;

    SELECT /*+ ALL_ROWS */
     receipt_no BULK COLLECT
    INTO   acct_bkr_rpno_len_tab
    FROM   bk_receipt_info
    WHERE  LENGTH(receipt_no) < 14;

    --noformat start
    FORALL i IN acct_bkr_rpno_len_tab.FIRST .. acct_bkr_rpno_len_tab.LAST
        UPDATE   bk_receipt_info a
           SET   a.receipt_no = (case when length(a.receipt_no) = 13 then LPAD (a.receipt_no, 14, '0') else TO_CHAR(a.receipt_no) end)
         WHERE   a.receipt_no = acct_bkr_rpno_len_tab (i);

    COMMIT;

    SELECT                                                 /*+ ALL_ROWS */
          receipt_no
      BULK   COLLECT
      INTO   acct_bkr_st_tab
      FROM   bk_receipt_info
     WHERE   LENGTH (status) < 4;

    FORALL i IN acct_bkr_st_tab.FIRST .. acct_bkr_st_tab.LAST
        UPDATE   bk_receipt_info a
           SET   a.status =
                     DECODE (a.status,
                             '1', 'ACTV',
                             --ACTIVE
                             '2', 'CLOS',
                             --CLOSED
                             '3', 'MATU',
                             --MATURED
                             '4', 'ACTV',
                             --New Record
                             '5', 'ACTZ',
                             --Active zero balance
                             '6', 'REST',
                             --RESTRICTED
                             '7', 'NOPO',
                             --NO POST
                             '8', 'COUN',
                             --Code unavailable
                             '9', 'DORM',
                             --DORMANT
                             '')
         WHERE   a.receipt_no = acct_bkr_st_tab (i);

    COMMIT;

    SELECT                                                 /*+ ALL_ROWS */
          receipt_no
      BULK   COLLECT
      INTO   acct_bkr_acno_len_tab
      FROM   bk_receipt_info
     WHERE   LENGTH (account_no) < 14;

    FORALL i IN acct_bkr_acno_len_tab.FIRST .. acct_bkr_acno_len_tab.LAST
        UPDATE   bk_receipt_info a
           SET   a.account_no = (case when length(a.account_no) = 13 then LPAD (a.account_no, 14, '0') else TO_CHAR(a.account_no) end)
         WHERE   a.receipt_no = acct_bkr_acno_len_tab (i);

    COMMIT;

    --noformat end

    --Cap nhap tai khoan da tat toan trong ngay thanh CLOS
    /*UPDATE bk_account_info a
    SET    a.status = 'CLOS'
    WHERE  (a.current_cash_value = 0 OR a.current_cash_value IS NULL)
    AND    a.acct_type = 'FD'
    AND    a.p_acct_no IS NOT NULL;*/
    --tam thoi delete cac tai khoan da tat toan

    g_error_level := 7;

    /*SELECT a.receipt_no BULK COLLECT
    INTO   l_bkr_acct_tab
    FROM   bk_receipt_info a
    WHERE  (a.status = 'CLOS');*/

    g_error_level := 8;


    SELECT /*+ ALL_ROWS */
     receipt_no BULK COLLECT
    INTO   acct_bkr_st_clos_tab
    FROM   bk_receipt_info
    WHERE  status = 'CLOS';

    FORALL i IN acct_bkr_st_clos_tab.FIRST .. acct_bkr_st_clos_tab.LAST
      DELETE FROM bk_receipt_info a
      WHERE  a.receipt_no = acct_bkr_st_clos_tab(i);

    COMMIT;

    g_error_level := 9;

    /*SELECT a.acct_no BULK COLLECT
    INTO   l_bk_acct_tab
    FROM   bk_account_info a
    WHERE  a.status = 'CLOS'
    AND    a.acct_type = 'FD';*/

    g_error_level := 10;


    SELECT /*+ ALL_ROWS */
     acct_no BULK COLLECT
    INTO   acct_bka_st_clos_tab
    FROM   bk_account_info
    WHERE  status = 'CLOS'
    AND    acct_type = 'FD';

    FORALL i IN acct_bka_st_clos_tab.FIRST .. acct_bka_st_clos_tab.LAST
      DELETE FROM bk_account_info a
      WHERE  a.acct_no = acct_bka_st_clos_tab(i);

    /*DELETE FROM bk_account_info a
    WHERE  ((a.status = 'CLOS' AND a.acct_type = 'FD') OR
    (a.acct_type = 'FD' AND a.principal_balance = 0 AND
    a.p_acct_no IS NOT NULL));*/
    COMMIT;
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdtnew_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdtnew_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;



  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync new fd account info (be created on day)
  -- ManhNV

  PROCEDURE proc_lntnew_sync IS
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count := 0;

    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    g_error_level := 1;


    --FOR i IN 1 .. g_loop_count


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_lntnew
              SELECT /*+ ALL_ROWS */
               b       .bkn,
               b.brn,
               b.accint, --LNPDUE.PDIINT,
               b.TYPE,
               b.cifno,
               b.lnnum,
               b.acctno,
               b.purcod,
               b.curtyp,
               b.orgamt,
               b.cbal,
               b.ysobal,
               b.billco,
               b.term,
               b.freq,
               b.frcode,
               b.ipfreq,
               b.ipcode,
               b.fulldt,
               b.status,
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
               b.acname,
               b.TYPE   produc_type,
               b.datopn,
               b.matdt,
               b.FRELDT,
               b.rate,
               trim(b.tmcode)
              FROM   RAWSTAGE.SI_DAT_LNTNEW@RAWSTAGE_PRO_CORE b
              WHERE  b.status <> 2
                    /*AND b.curtyp = 'VND'*/
              AND    b.cifno BETWEEN g_min_count AND g_max_count;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    /*        EXIT;
        \*ADVICE(2988): An EXIT statement is used in a FOR loop [501] *\
      EXCEPTION
        WHEN OTHERS
        \*ADVICE(2991): A WHEN OTHERS clause is used in the exception section
                          without any other specific handlers [201] *\
         THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;*/

    g_error_level := 2;


    /*INSERT INTO sync_cif_n
    (cif_no,
     status)
    SELECT a.cif_no,
           'N'
    FROM   bc_user_info a;*/

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cifno)
    INTO   g_cif_count
    FROM   sync_lntnew a;

    SELECT MIN(a.cifno)
    INTO   g_min_count
    FROM   sync_lntnew a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    INSERT INTO sync_cif_n
      SELECT a.cif_no,
             'Y'
      FROM   (
              SELECT cif_no
              FROM   bb_corp_info) a;

    --noformat start
    g_error_level := 5;

    LOOP
        BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cifno BULK COLLECT
            INTO   cif_tab
            FROM   sync_lntnew a
            WHERE  a.cifno BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_account_info c
                 USING   (SELECT /*+ INDEX(sync_lntnew, IDX_SYNC_LNTNEW) */
                                b  .bkn,
                                   b.brn,
                                   b.accint,              --LNPDUE.PDIINT,
                                   b.TYPE,
                                   b.cifno,
                                   b.lnnum,
                                   (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                   b.purcod,
                                   b.curtyp,
                                   b.orgamt,
                                   b.cbal,
                                   b.ysobal,
                                   b.billco,
                                   b.term,
                                   b.freq,
                                   b.frcode,
                                   b.ipfreq,
                                   b.ipcode,
                                   b.fulldt,
                                   DECODE (b.status,
                                           '1', 'ACTV',
                                           --ACTIVE
                                           '2', 'CLOS',
                                           --CLOSED
                                           '3', 'MATU',
                                           --MATURED
                                           '4', 'ACTV',
                                           --'NEWR', --New Record
                                           '5', 'ACTZ',
                                           --Active zero balance
                                           '6', 'REST',
                                           --RESTRICTED
                                           '7', 'NOPO',
                                           --NO POST
                                           '8', 'COUN',
                                           --Code unavailable
                                           '9', 'DORM',
                                           --DORMANT
                                           'ACTV')
                                       status,
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
                                   TRIM (b.produc_type) product_type,
                                   b.matdt,
                                   b.datopn,
                                   b.FRELDT,
                                   b.rate,
                                   b.tmcode
                            FROM   sync_lntnew b
                           WHERE   b.cifno BETWEEN g_min_count
                                                   AND  g_max_count
                                         AND b.acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                                   AND b.status <> 2
                                   AND TRIM (b.cifno) IN
                                              (SELECT   cif_no
                                                 FROM   sync_cif_n
                                               )
                                    --AND    b.cifno = cif_tab(i)
                                                                   ) a
                    ON   (a.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET c.status = a.status
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
                                     c.overdraft_limit, /*c.available_limit,*/
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
                    VALUES   ('302',                              --a.bkn,
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),               --a.brn,
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),               --a.brn,
                              a.accint,
                              a.cifno,
                              TRIM (a.lnnum),
                              (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                              TRIM (a.purcod),
                              TRIM (a.curtyp),
                              a.orgamt,
                              a.cbal,
                              a.ysobal + a.billco,             /*a.TERM,*/
                              TRIM (a.freq),
                              a.ipfreq,
                              DECODE (a.fulldt,
                                      0, NULL,
                                      TO_DATE (a.fulldt, 'yyyyddd')),
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
                              a.drlimt,                      /*a.DRLIMT,*/
                              a.hold,
                              TRIM (a.accmlc),
                              a.comacc,
                              a.othchg,
                              'LN',
                              TRIM (a.acname),
                              TRIM (a.product_type),
                              DECODE (a.datopn,
                                      0, NULL,
                                      TO_DATE (a.datopn, 'yyyyddd')),
                              DECODE (a.matdt,
                                      0, NULL,
                                      TO_DATE (a.matdt, 'yyyyddd')),
                              DECODE (a.FRELDT, 0, NULL, TO_DATE (a.FRELDT, 'yyyyddd')),
                                      a.rate,
                                      a.term || a.tmcode
                                      );

            COMMIT;

            g_min_count := g_max_count;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_count > g_cif_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_PARAM';
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_lntnew_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_lntnew_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;



  ----------------------------------------------------------------------------
  -- Date:16/05/2011
  -- Comments: Sync ca,sa account info (account balance, The other changing information ...)
  -- ManhNV

  PROCEDURE proc_ddmemo_sync IS
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;

    g_limit_count := 200000;

    g_min_count := 0;

    g_max_count := 0;

    g_cif_count := 0;

    g_error_count := 0;

    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    g_error_level := 1;


    /*INSERT INTO sync_account_info a
    (SELECT bai.cif_no,
            bai.acct_no
     FROM   bk_account_info bai
     WHERE  bai.cif_no IN (SELECT cif_no
                           FROM   bc_user_info));*/

    g_error_level := 2;


    --FOR i IN 1 .. g_loop_count


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;

            INSERT INTO sync_ddmemo
              ( --bankno,
               --branch,
               acctno,
               --actype,
               cifno,
               status,
               hold,
               cbal,
               odlimt,
               --sccode,
               acname)
              (SELECT /*+ ALL_ROWS */
               --NULL,
               --a.bankno,
               --NULL,
               --a.branch,
                a.acctno,
                --NULL,
                --TRIM(a.actype),
                a.cifno,
                a.status,
                a.hold,
                a.cbal,
                a.odlimt,
                --NULL,
                --TRIM(a.sccode),
                TRIM(a.acname)
               FROM   RAWSTAGE.SI_DAT_DDMEMO_N@RAWSTAGE_PRO_CORE a
               /*WHERE  LPAD(a.acctno,
               14,
               '0') IN (SELECT acct_no
                        FROM   sync_account_info)*/
               WHERE  a.cifno BETWEEN g_min_count AND g_max_count);

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;

              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;

          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;

    /*        --COMMIT;
        EXIT;
        \*ADVICE(3343): An EXIT statement is used in a FOR loop [501] *\
      EXCEPTION
        WHEN OTHERS
        \*ADVICE(3346): A WHEN OTHERS clause is used in the exception section
                          without any other specific handlers [201] *\
         THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;*/


    g_error_level := 3;


    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   min_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            \*ADVICE(3363): Use of ROWID or UROWID [113] *\
            FROM   sync_ddmemo
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 1
    \*ADVICE(3367): This item has not been declared, or it refers to a label [131] *\
    ORDER  BY 1;*/

    g_error_level := 4;


    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   max_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            \*ADVICE(3378): Use of ROWID or UROWID [113] *\
            FROM   sync_ddmemo
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 0
    \*ADVICE(3382): This item has not been declared, or it refers to a label [131] *\
    UNION
    SELECT \*+ ALL_ROWS *\
     MAX(ROWID)
    FROM   sync_ddmemo;*/


    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    SELECT MAX(a.cifno)
    INTO   g_cif_count
    FROM   sync_ddmemo a;

    SELECT MIN(a.cifno)
    INTO   g_min_count
    FROM   sync_ddmemo a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
    g_error_level := 5;

    LOOP
        BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cifno BULK COLLECT
            INTO   cif_tab
            FROM   sync_ddmemo a
            WHERE  a.cifno BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_account_info c
                 USING   (SELECT /*+ INDEX(sync_ddmemo, IDX_SYNC_DDMEMO) */
                                   --a.bankno,
                                   --a.branch,
                                   (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end) acctno,
                                   --a.actype,
                                   --a.cifno,
                                   DECODE (a.status,
                                           '1', 'ACTV',
                                           --ACTIVE
                                           '2', 'CLOS',
                                           --CLOSED
                                           '3', 'MATU',
                                           --MATURED
                                           '4', 'ACTV',
                                           --'NEWR', --New Record
                                           '5', 'ACTZ',
                                           --Active zero balance
                                           '6', 'REST',
                                           --RESTRICTED
                                           '7', 'NOPO',
                                           --NO POST
                                           '8', 'COUN',
                                           --Code unavailable
                                           '9', 'DORM',
                                           --DORMANT
                                           '')
                                       status,
                                   a.hold,
                                   a.cbal,
                                   a.odlimt,
                                   a.sccode,
                                   a.acname
                            FROM   sync_ddmemo a
                           WHERE   a.acctno <> 0
                                   AND TRIM (a.status) IS NOT NULL
                                   /*AND    LPAD(a.acctno,
                                               14,
                                               '0') IN
                                          (SELECT acct_no
                                            FROM   bk_account_info bai
                                            WHERE  bai.cif_no IN
                                                   (SELECT cif_no
                                                    FROM   bc_user_info))*/
                                   AND a.cifno BETWEEN g_min_count
                                                   AND  g_max_count --AND    a.cifno = cif_tab(i)
                --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                         ) src
                    ON   (src.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET --c.org_no = LPAD(a.branch, 3, '0'),--a.branch,
                    --c.branch_no = LPAD(a.branch, 3, '0'),--a.branch,
                    --         c.currency_code     = trim(a.DDCTYP),
                    --         c.bank_no           = a.BANKNO,
                    c.status = src.status,
                    c.hold_amount = src.hold,
                    c.ledger_balance = src.cbal,
                    c.available_balance = src.cbal - src.hold + src.odlimt,
                    c.overdraft_limit = src.odlimt,
                    --        c.interest_rate     = a.WHHIRT,
                    c.acct_name = trim(src.acname);

            --        c.product_type      = a.product_type,
            --        c.passbook_no       = a.passbook_no,
            --        c.acct_type         = decode(a.ACTYPE,'S','SA','CA')
            COMMIT;

            g_min_count := g_max_count;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_count > g_cif_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    --   EXECUTE IMMEDIATE 'alter session close database link DBLINK_PARAM';
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddmemo_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddmemo_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;



  ----------------------------------------------------------------------------
  -- Date:16/05/2011
  -- Comments: Sync fd account info (account balance, The other changing information ...)
  -- ManhNV

  PROCEDURE proc_cdmemo_sync IS
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;


    g_error_level := 1;


    g_error_count := 0;

    /*INSERT INTO sync_account_info a
    (SELECT NULL,
            bai.receipt_no
     FROM   bk_receipt_info bai);*/

    g_error_level := 2;


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_temp;

        INSERT INTO sync_cdmemo
          (acctno,
           curtyp,
           cbal,
           accint,
           penamt,
           hold,
           wdrwh,
           cdnum,
           status)
          (SELECT /*+ ALL_ROWS */
            b.acctno,
            TRIM(b.curtyp),
            b.cbal,
            b.accint,
            b.penamt,
            b.hold,
            b.wdrwh,
            b.cdnum,
            b.status
           FROM   RAWSTAGE.SI_DAT_CDMEMO@RAWSTAGE_PRO_CORE b /*WHERE  LPAD(b.acctno,
                                                                                              14,
                                                                                              '0') IN (SELECT acct_no
                                                                                                       FROM   sync_account_info)*/
           );

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;

    g_error_level := 3;


    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   min_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            \*ADVICE(3565): Use of ROWID or UROWID [113] *\
            FROM   sync_cdmemo
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 1
    \*ADVICE(3569): This item has not been declared, or it refers to a label [131] *\
    ORDER  BY 1;*/

    g_error_level := 4;


    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   max_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            \*ADVICE(3580): Use of ROWID or UROWID [113] *\
            FROM   sync_cdmemo
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 0
    \*ADVICE(3584): This item has not been declared, or it refers to a label [131] *\
    UNION
    SELECT \*+ ALL_ROWS *\
     MAX(ROWID)
    FROM   sync_cdmemo;*/


    g_error_level := 5;


    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_receipt_info c
    USING (SELECT /*+ INDEX(sync_cdmemo, IDX_SYNC_CDMEMO) */
            (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end)
             acctno,
            --b.curtyp,
            b.cbal,
            b.accint,
            --b.penamt,
            --b.hold,
            --b.wdrwh,
            --b.cdnum,
            DECODE(b.status,
                   '1',
                   'ACTV',
                   --ACTIVE
                   '2',
                   'CLOS',
                   --CLOSED
                   '3',
                   'MATU',
                   --MATURED
                   '4',
                   'ACTV',
                   --'NEWR', --New Record
                   '5',
                   'ACTZ',
                   --Active zero balance
                   '6',
                   'REST',
                   --RESTRICTED
                   '7',
                   'NOPO',
                   --NO POST
                   '8',
                   'COUN',
                   --Code unavailable
                   '9',
                   'DORM',
                   --DORMANT
                   '') status
           FROM   sync_cdmemo b
           WHERE  b.acctno <> 0
           AND    TRIM(b.status) IS NOT NULL
           /*AND    LPAD(b.acctno,
           14,
           '0') IN (SELECT receipt_no
                     FROM   bk_receipt_info)*/
           --AND    b.ROWID between min_rid_tab(i) and max_rid_tab(i)
           ) a
    ON (a.acctno = c.receipt_no)
    WHEN MATCHED THEN
      UPDATE
      SET    --            c.bank_no               =   a.BANKNO,
             --            c.org_no                =   a.BRN,
             --            c.term                  =   a.CDTERM, Chua hieu term
             -- c.currency_code = a.curtyp,
             --            c.original_balance      =   a.ORGBAL,
             --c.principal_balance  = a.cbal,
             --c.accured_interest   = a.accint,
             --c.penalty_amount     = a.penamt,
             --c.hold_amount        = a.hold,
             --c.current_cash_value =
             --(a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
             --            c.issued_date           =   to_date(a.ISSDT6,'ddMMyy'),
             --            c.maturity_date         =   to_date(a.MATDT6,'ddMMyy'),
             --            c.times_nenewed_count   =   a.RNWCTR,
             --c.status = a.status,
             --            c.product_type          =   a.product_type,
             --c.acct_type = 'FD',
             --            c.acct_name             =   trim(a.ACNAME),
             --c.p_acct_no = a.cdnum;
             --c.product_code = a.product_type,
               c.principal = a.cbal, --a.orgbal,
             --c.interest_rate   = a.rate,
             c.interest_amount = a.accint,
             --c.term            = a.cdterm,
             --c.opening_date    = TO_DATE(a.issdt,
             --                            'yyyyddd'),
             --c.settlement_date = TO_DATE(a.matdt,
             --                            'yyyyddd'),
             --c.is_rollout_interest = TRIM(a.renew),
             --c.interest_receive_account = a.dactn,
             c.status = a.status;

    /* c.account_no               = LPAD(a.cdnum,
    14,
    '0'); */

    g_error_level := 6;


    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_account_info c
    USING (SELECT /*+ INDEX(sync_cdmemo, IDX_SYNC_CDMEMO) */
            (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end)    
             acctno,
            --b.curtyp,
            b.cbal,
            b.accint,
            b.penamt,
            b.hold,
            b.wdrwh,
            b.cdnum,
            DECODE(b.status,
                   '1',
                   'ACTV',
                   --ACTIVE
                   '2',
                   'CLOS',
                   --CLOSED
                   '3',
                   'MATU',
                   --MATURED
                   '4',
                   'ACTV',
                   --'NEWR', --New Record
                   '5',
                   'ACTZ',
                   --Active zero balance
                   '6',
                   'REST',
                   --RESTRICTED
                   '7',
                   'NOPO',
                   --NO POST
                   '8',
                   'COUN',
                   --Code unavailable
                   '9',
                   'DORM',
                   --DORMANT
                   '') status
           FROM   sync_cdmemo b
           WHERE  b.acctno <> 0
           AND    TRIM(b.status) IS NOT NULL
           /*AND    LPAD(b.acctno,
           14,
           '0') IN (SELECT receipt_no
                     FROM   bk_receipt_info)*/ --AND    b.ROWID between min_rid_tab(i) and max_rid_tab(i)
           /*AND    LPAD(b.acctno,
           14,
           '0') IN (SELECT acct_no
                     FROM   sync_account_info)*/
           ) a
    ON (a.acctno = c.acct_no)
    WHEN MATCHED THEN
      UPDATE
      SET    --            c.bank_no               =   a.BANKNO,
             --            c.org_no                =   a.BRN,
             --            c.term                  =   a.CDTERM, Chua hieu term
             --  c.currency_code = a.curtyp,
                c.original_balance = a.cbal,
             c.principal_balance  = a.cbal,
             c.accured_interest   = a.accint,
             c.penalty_amount     = a.penamt,
             c.hold_amount        = a.hold,
             c.current_cash_value =
             (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
             --            c.issued_date           =   to_date(a.ISSDT6,'ddMMyy'),
             --            c.maturity_date         =   to_date(a.MATDT6,'ddMMyy'),
             --            c.times_nenewed_count   =   a.RNWCTR,
             c.status = a.status,
             --            c.product_type          =   a.product_type,
             c.acct_type = 'FD',
             --            c.acct_name             =   trim(a.ACNAME),
             c.p_acct_no = a.cdnum;

    COMMIT;
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdmemo_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdmemo_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;



  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync loan account info (account balance, The other changing information ...)
  -- ManhNV

  PROCEDURE proc_lnmemo_sync IS
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;


    g_error_level := 1;


    g_error_count := 0;

    /*INSERT INTO sync_account_info a
    (SELECT bai.cif_no,
            bai.acct_no
     FROM   bk_account_info bai
     WHERE  bai.cif_no IN (SELECT cif_no
                           FROM   bc_user_info));*/

    g_error_level := 2;


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_temp;

        INSERT INTO sync_lnmemo
          SELECT /*+ ALL_ROWS */
           b.accint,
           b.acctno,
           b.curtyp,
           b.cbal,
           b.billco,
           b.bilprn,
           b.bilint,
           b.billc,
           b.bilesc,
           b.biloc,
           b.bilmc,
           b.drlimt,
           b.hold,
           b.comacc,
           b.othchg
          FROM   RAWSTAGE.SI_DAT_LNMEMO@RAWSTAGE_PRO_CORE b /*WHERE  LPAD(b.acctno,
                                                                                          14,
                                                                                          '0') IN (SELECT acct_no
                                                                                                   FROM   sync_account_info)*/
          ;

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;


    g_error_level := 3;


    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   min_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            \*ADVICE(3851): Use of ROWID or UROWID [113] *\
            FROM   sync_lnmemo
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 1
    \*ADVICE(3855): This item has not been declared, or it refers to a label [131] *\
    ORDER  BY 1;*/

    g_error_level := 4;


    /*SELECT \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   max_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            \*ADVICE(3866): Use of ROWID or UROWID [113] *\
            FROM   sync_lnmemo
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 0
    \*ADVICE(3870): This item has not been declared, or it refers to a label [131] *\
    UNION
    SELECT \*+ ALL_ROWS *\
     MAX(ROWID)
    FROM   sync_lnmemo;*/

    /*INSERT INTO sync_cif_n
      SELECT a.cif_no,
             'Y'
      FROM   (SELECT cif_no
              FROM   bc_user_info
              UNION
              SELECT cif_no
              FROM   bb_corp_info) a;*/

    g_error_level := 5;


    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_account_info c
    USING (SELECT /*+ INDEX(sync_lnmemo, IDX_SYNC_LNMEMO) */
            b.accint,
            (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end)
             acctno,
            b.curtyp,
            b.cbal,
            b.billco,
            b.bilprn,
            b.bilint,
            b.billc,
            b.bilesc,
            b.biloc,
            b.bilmc,
            b.drlimt,
            b.hold,
            b.comacc,
            b.othchg
           FROM   sync_lnmemo b
           WHERE  b.acctno <> 0 --AND    b.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           /*AND    LPAD(b.acctno,
                       14,
                       '0') IN
                  (SELECT bai.acct_no
                    FROM   bk_account_info bai
                    WHERE  bai.cif_no IN (SELECT cif_no
                                          FROM   sync_cif_n))*/
                                                                                    ) a
    ON (a.acctno = c.acct_no)
    WHEN MATCHED THEN
      UPDATE
      SET    c.accured_interest = a.accint,
             c.currency_code    = TRIM(a.curtyp),
             c.os_principal     = a.cbal,
             --            c.os_balance          = a.ysobal + a.billco,-- trong memo khong co ysobal
             c.billed_total_amount = a.bilprn + a.bilint + a.billc +
                                     a.bilesc + a.biloc + a.bilmc,
             c.billed_principal    = a.bilprn,
             c.billed_interest     = a.bilint,
             c.billed_late_charge  = a.billc,
             c.overdraft_limit     = a.drlimt,
             c.hold_amount         = a.hold,
             c.accrued_common_fee  = a.comacc,
             c.other_charges       = a.othchg;


    COMMIT;
    --EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_lnmemo_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_lnmemo_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;



  ----------------------------------------------------------------------------
  -- Date:16/05/2011
  -- Comments: sync passbook no
  -- ManhNV

  PROCEDURE proc_passbook_no_sync IS
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count := 10;


    g_error_level := 1;


    g_error_count := 0;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info));

    g_error_level := 2;


    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_temp;

        INSERT INTO sync_tmpbmast
          SELECT /*+ ALL_ROWS */
           a.tmbmrser,
           a.tmbmacct
          FROM   stdattrn.tmpbmast@dblink_data1 a
          WHERE  (case when length(a.tmbmacct) = 13 then LPAD (a.tmbmacct, 14, '0') else TO_CHAR(a.tmbmacct) end) IN (SELECT acct_no
                               FROM   sync_account_info);

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;

    g_error_level := 5;


    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_account_info c
    USING (SELECT /*+ INDEX(sync_tmpbmast, IDX_SYNC_TMPBMAST) */
            a.tmbmrser,
            a.tmbmacct
           FROM   sync_tmpbmast a --WHERE  a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           ) a
    ON (a.tmbmacct = TO_NUMBER(c.acct_no))
    WHEN MATCHED THEN
      UPDATE
      SET    c.passbook_no = a.tmbmrser;

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    /*ebank_common_sync.proc_update_status_job_sync('Y',
    'SYNC_ACCOUNT_PASSBOOK_NO');*/
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_passbook_no_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      /*ebank_common_sync.proc_update_status_job_sync('N',
      'SYNC_ACCOUNT_PASSBOOK_NO');*/
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_passbook_no_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;


  PROCEDURE proc_ddmaster IS
  BEGIN
  execute immediate 'truncate table DDMASTER';
  insert into DDMASTER (select ACCTNO,actype,cbal from RAWSTAGE.SI_DAT_DDMAST@RAWSTAGE_PRO_CORE where status != 2);
  insert into DDMASTER (select ACCTNO,actype,cbal from RAWSTAGE.SI_DAT_DDTNEW@RAWSTAGE_PRO_CORE where status != 2);
  END;

  PROCEDURE proc_cdmaster IS
  BEGIN
      execute immediate 'truncate table CDMASTER';
  insert into CDMASTER (select ACCTNO,actype,cbal from STAGING.SI_DAT_CDMAST@STAGING_PRO_CORE where status != 2);
  insert into CDMASTER (select ACCTNO,actype,cbal from RAWSTAGE.SI_DAT_CDTNEW@RAWSTAGE_PRO_CORE where status != 2);
  END;

    PROCEDURE proc_lnmaster IS
  BEGIN
  execute immediate 'truncate table LNMASTER';
  insert into LNMASTER (select ACCTNO,actype,cbal from RAWSTAGE.SI_DAT_LNMAST@RAWSTAGE_PRO_CORE where status != 2);
  insert into LNMASTER (select ACCTNO,actype,cbal from RAWSTAGE.SI_DAT_LNTNEW@RAWSTAGE_PRO_CORE where status != 2);
  END;



END;

/
