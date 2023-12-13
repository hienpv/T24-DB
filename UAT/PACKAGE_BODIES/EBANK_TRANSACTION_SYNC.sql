--------------------------------------------------------
--  DDL for Package Body EBANK_TRANSACTION_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."EBANK_TRANSACTION_SYNC" 
/* Formatted on 12/16/2011 5:13:24 PM (QP5 v5.126) */
 IS
  v_start_date      DATE;
  p_min_value       NUMBER;
  l_val             NUMBER;
  v_checkpoint_time DATE;
  v_is_sync         CHAR(1);

  TYPE core_sn_list IS TABLE OF VARCHAR2(20) INDEX BY BINARY_INTEGER;

  core_sn_tab core_sn_list;

  g_error_level NUMBER;

  g_loop_count      NUMBER;
  g_limit_time      NUMBER;
  g_min_time        NUMBER;
  g_max_time        NUMBER;
  g_time_count      NUMBER;
  g_error_sub_count NUMBER;
  g_error_count     NUMBER;
  v_checkpoint_date NUMBER;

  ----------------------------------------------------------------------------
  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync transaction history(on day) ko dung check point
  --ManhNV
  PROCEDURE proc_tmtran_onday_sync IS
    v_checkpoint_number NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    v_is_sync         := 'N';
    g_loop_count      := 10;

    g_error_level := 1;

    SELECT TO_NUMBER(TO_CHAR(a.sync_end_time,
                             'yyyy') || TO_CHAR(a.sync_end_time,
                                                'mm') ||
                     TO_CHAR(a.sync_end_time,
                             'dd') || TO_CHAR(a.sync_end_time,
                                              'hh24') ||
                     TO_CHAR(a.sync_end_time,
                             'mi') || TO_CHAR(a.sync_end_time,
                                              'ss'))
    INTO   v_checkpoint_number
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'TMTRAN';

    g_error_level := 2;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_temp;

        INSERT INTO sync_tmtrans
          SELECT /*+ ALL_ROWS */
           trim(a.tmtxcd),
           a.tmresv07,
           a.tmdorc,
           a.tmtxamt,
           a.tmglcur,
           a.tmorgamt,
           a.tmtellid,
           a.tmefth,
           a.tmacctno,
           SYSDATE, --tminsdate,
           a.tmtellid,
           a.tmtxseq,
           a.tmtxstat,
           a.tmhosttxcd,
           a.tmapptype,
           a.tmeqvtrn,
           a.tmibttrn,
           a.tmsumtrn,
           '1', --tmtype
           'SUCC', --tmsts,
           a.tmentdt7,
           --'TM' || seq_core_sn_tm.nextval
           a.tmeffdt7,
           NULL,
           a.tmsseq,
           a.tmtiment
          FROM   STG.SI_DAT_TMTRAN@STAGING_PRO a
          WHERE  a.tmapptype <> 'G' /*NOT IN ('S',
                                                                                                                                                                                                                                                                                                                                     'G')*/
                /*AND    LENGTH(rtrim(a.tmresv07)) = 14*/
          AND    RTRIM(a.tmresv07) IS NOT NULL
          AND    TO_NUMBER(a.tmresv07) >= v_checkpoint_number
          AND    (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
          AND    (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
          AND    (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
          AND    a.tmhosttxcd NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
                --AND    (a.tmtxstat IS NULL OR a.tmtxstat NOT IN ('CE'))
                --AND    a.tmtxstat <> 'CE'       --giao dich huy
          AND    a.tmapptype IS NOT NULL
          AND    a.tmdorc IN ('D',
                              'C');

        --AND    a.tmtxstat NOT IN ('PP','PT');
        /*AND    trunc(to_date(a.tmentdt7, 'yyyyddd')) = trunc(SYSDATE);*/
        -- AND    TRUNC(TO_DATE(b.message_date,
        --'yyyymmdd')) = TRUNC(SYSDATE)
        -- AND    TO_DATE(a.tmresv07,
        --'YYYYMMDDHH24MISS') > v_checkpoint_time
        /* AND    TO_DATE(a.tmresv07,
                                     'yyyyMMddHH24miss') > v_checkpoint_time
        AND    TRUNC(TO_DATE(a.tmresv07,
                                                 'yyyyMMddHH24miss')) = TRUNC(SYSDATE) */
        --; --1201
        --FROM   svdatpv51.TMTRAN@DBLINK_DATA a;
        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;


    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_tranmap;

        INSERT INTO sync_tranmap
          SELECT /*+ ALL_ROWS */
           a.tran_sn,
           a.teller_id,
           a.host_tran_sn,
           a.host_real_date,
           a.sender_id
          FROM   bec.bec_msglog@dblink_tranmap a
          --FROM   bec.bec_msglog2 a
          WHERE  a.sorn = 'Y' and a.resp_code in ('0','999')
          AND    TRUNC(TO_DATE(a.message_date,
                               'yyyymmddhh24mi')) >= TRUNC(SYSDATE)
          AND    LENGTH(a.tran_sn) < 21;

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_tranmap;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 8;

    INSERT INTO sync_account_info
      SELECT cif_no,
             acct_no
      FROM   bk_account_info a
      WHERE  a.cif_no IN (
                          SELECT cif_no
                          FROM   bb_corp_info);

    FOR j IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_bulk_merge;

        --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
        MERGE INTO bk_account_history c
        USING (SELECT /*+ INDEX(sync_tmtrans, IDX_SYNC_TMTRAN) */
               --(TRIM(a.tmtellid) || TRIM(a.tmtxseq) || TRIM(a.tmentdt7)),
               --a.core_sn,
               --('TM' || seq_core_sn_tm.nextval) AS core_sn1,
                TO_DATE((a.tmentdt7 || ':' ||
                        LPAD(a.tmtiment,
                              6,
                              '0')),
                        'yyyyddd:hh24miss') AS post_date,
                TO_DATE((a.tmeffdt7 || ':' ||
                        LPAD(a.tmtiment,
                              6,
                              '0')),
                        'yyyyddd:hh24miss') AS tran_date,
                /*DECODE(length(TRIM(TMEFFDT7)), 7, TO_DATE(TMEFFDT7, 'yyyyddd'),
                             NULL) AS tran_date,
                DECODE(length(TRIM(tmentdt7)), 7, TO_DATE(tmentdt7, 'yyyyddd'),
                             NULL) AS post_date,*/
                TRIM(a.tmdorc) AS tmdorc,
                a.tmtxamt,
                TRIM(a.tmglcur) AS tmglcur,
                a.tmorgamt,
                /* DECODE(b.sender_id,
                NULL,
                'CNT',
                b.sender_id) AS channel_id, */
                --DECODE(e.staff_name, NULL, 'CNT', 'IBS'),
                --TRIM(a.tmefth) AS tmefth,
                SUBSTR(a.tmefth,
                       11,
                       LENGTH(a.tmefth)) AS tmefth,
                a.tmacctno,
                SYSDATE AS tminsdate,
                TRIM(a.tmtellid) AS tmtellid,
                a.tmtxseq,
                /*'1',
                'SUCC',
                'SYNC', */
                b.tran_sn,
                TRIM(a.tmtxstat) AS tmtxstat,
                a.tmentdt7,
                a.tmresv07,
                d.tran_service_code,
                --e.staff_name,
                a.tmsseq,
                a.tmhosttxcd,
                b.sender_id,
                TRIM(a.tmapptype) AS tmapptype,
                trim(a.tmtxcd) tmtxcd --anhnt6
               FROM   sync_tmtrans   a,
                      sync_tranmap   b,
                      sync_tran_code d /*,
                                                                                                                                                                                                                                                                                                                                                                    sync_teller    e*/
               WHERE  TRIM(a.tmtellid) = b.teller_id(+)
               AND    TRIM(a.tmtxseq) = b.host_tran_sn(+)
               AND    TRUNC(TO_DATE(a.tmentdt7,
                                    'yyyyddd')) =
                      TRUNC(TO_DATE(b.host_real_date(+),
                                     'yyyyddd'))
                     /* AND    EXISTS
                     (SELECT
                                     b.cif_no
                                    FROM   bk_account_info b
                                    WHERE  EXISTS (SELECT 1
                                                    FROM   bk_cif bc
                                                    WHERE  bc.cif_no = b.cif_no)) */
                     /*               AND    LPAD(a.tmacctno,
                          14,
                          '0') IN
                     (SELECT bai.acct_no
                       FROM   bk_account_info bai
                       WHERE  bai.cif_no IN
                              (SELECT cif_no
                               FROM   bc_user_info))*/
               AND    TRIM(a.tmtxcd) = d.tran_code(+)
               AND    EXISTS
                (SELECT bai.acct_no
                       FROM   sync_account_info bai
                       WHERE  bai.acct_no = (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end)) /*AND    TO_DATE(a.tmresv07,
                                                                                                                                                                                                                              'yyyyMMddHH24miss') > v_checkpoint_time*/
               --to_date('14/11/2011 18:16:53','dd/mm/yyyy hh24:mi:ss')--v_checkpoint_time
               /* AND    TRUNC(TO_DATE(a.tmresv07,
               'yyyyMMddHH24miss')) = TRUNC(SYSDATE) */
               --AND    TRIM(a.tmtellid) = e.staff_name(+)
               --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
               /* AND    (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
               AND    (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
               AND    (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
               AND    a.tmapptype NOT IN ('S',
                                                                      'G')
               AND    a.tmhosttxcd NOT IN (77,
                                                                       129,
                                                                       178,
                                                                       179,
                                                                       185)
                           --AND    (a.tmtxstat IS NULL OR a.tmtxstat NOT IN ('CE'))
               AND    a.tmtxstat <> 'CE' --giao dich huy
               AND    a.tmapptype IS NOT NULL
               AND    a.tmdorc IN ('D',
                                                       'C')
               AND    LENGTH(rtrim(a.tmresv07)) = 14
               AND    LENGTH(rtrim(a.tmresv07)) IS NOT NULL
                           -- AND    TRUNC(TO_DATE(b.message_date,
                           --'yyyymmdd')) = TRUNC(SYSDATE)
                           -- AND    TO_DATE(a.tmresv07,
                           --'YYYYMMDDHH24MISS') > v_checkpoint_time
               AND    TO_DATE(a.tmresv07,
                                              'yyyyMMddHH24miss') > v_checkpoint_time
               AND    TRUNC(TO_DATE(a.tmresv07,
                                                          'yyyyMMddHH24miss')) = TRUNC(SYSDATE) */
               ) src
        ON (c.teller_id = src.tmtellid AND c.tm_seq = src.tmtxseq AND TRUNC(c.post_time) = TRUNC(src.post_date) AND c.rollout_acct_no = LPAD(src.tmacctno, 14, '0') AND c.dc_sign = TRIM(src.tmdorc) AND c.tran_device = TRIM(src.tmsseq) AND c.device_no = TRIM(src.tmhosttxcd))
        WHEN MATCHED THEN
          UPDATE
          SET    c.status            = DECODE(src.tmtxstat,
                                              'CE',
                                              'FAIL',
                                              'SUCC'),
                 c.tran_service_code = DECODE(src.tran_service_code,
                                              NULL,
                                              'RTR001',
                                              'RTR002'), /* DECODE(src.tmtxstat,'','FAIL','PP','PEND','PT','SUCC','SUCC') */
                 --c.tran_sn           = src.tran_sn,
                 /*c.channel = DECODE(src.staff_name,
                 NULL,
                 'CNT',
                 'IBS'),*/
                 c.channel = DECODE(src.sender_id,
                                    NULL,
                                    'CNT',
                                    src.sender_id),
                 c.remark  = src.tmefth --,
          --c.tran_device = TRIM(src.tmsseq),
          --c.device_no = TRIM(src.tmhosttxcd)

















        WHEN NOT MATCHED THEN
          INSERT
            (c.core_sn,
             c.tran_time,
             c.post_time,
             c.dc_sign,
             c.amount,
             c.currency_code,
             c.pre_balance,
             c.channel,
             c.remark,
             c.rollout_acct_no,
             c.insert_date,
             c.teller_id,
             c.tm_seq,
             c.sync_type,
             c.status,
             c.tc_code,
             c.tran_sn,
             c.tran_service_code,
             c.tran_device,
             c.device_no,
             c.trace_code --anhnt6
             )
          VALUES
            ( --src.core_sn1,
             TRIM(src.tmresv07) || seq_core_sn.NEXTVAL,
             --TRIM(src.tmdorc) || TRIM(src.tmtellid) || TRIM(src.tmtxseq) ||
             --TRIM(src.tmentdt7),
             src.tran_date,
             src.post_date,
             src.tmdorc,
             src.tmtxamt,
             src.tmglcur,
             src.tmorgamt,
             --src.channel_id,
             /*DECODE(src.staff_name,
             NULL,
             'CNT',
             'IBS'),*/
             DECODE(src.sender_id,
                    NULL,
                    'CNT',
                    src.sender_id),
             src.tmefth,
             (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end),
             src.tminsdate,
             src.tmtellid,
             src.tmtxseq,
             --'0',
             DECODE(src.tmapptype,
                    'D',
                    '6',
                    'T',
                    '7',
                    'L',
                    '8',
                    'S',
                    '6',
                    '0'),
             DECODE(src.tmtxstat,
                    'CE',
                    'FAIL',
                    'SUCC'),
             --'SUCC'
             /* DECODE(src.tmtxstat,
             '',
             'FAIL',
             'PP',
             'PEND',
             'PT',
             'SUCC',
             'SUCC') ,*/
             'SYNC',
             src.tran_sn,
             DECODE(src.tran_service_code,
                    NULL,
                    'RTR001',
                    'RTR002'),
             TRIM(src.tmsseq),
             TRIM(src.tmhosttxcd),
             src.tmtxcd --anhnt6
             );

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_bulk_merge;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 9;

    FOR j IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_merge_bc;

        --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
--        MERGE INTO bc_transfer_history c
--        USING (SELECT b.tran_sn,
--                      TRIM(a.tmtxstat) AS tmtxstat,
--                      a.tmentdt7,
--                      a.tmresv07,
--                      a.tmacctno,
--                      a.tmtxseq,
--                      a.tmtxamt
--               FROM   sync_tmtrans a,
--                      sync_tranmap b
--               WHERE  TRIM(a.tmtellid) = b.teller_id(+)
--               AND    TRIM(a.tmtxseq) = b.host_tran_sn(+)
--               AND    TRUNC(TO_DATE(a.tmentdt7,
--                                    'yyyyddd')) =
--                      TRUNC(TO_DATE(b.host_real_date(+),
--                                     'yyyyddd'))
--                     /*AND    LPAD(a.tmacctno,
--                          14,
--                          '0') IN
--                     (SELECT bai.acct_no
--                       FROM   bk_account_info bai
--                       WHERE  bai.cif_no IN
--                              (SELECT cif_no
--                               FROM   bc_user_info))*/
--               AND    EXISTS
--                (SELECT bai.acct_no
--                       FROM   sync_account_info bai
--                       WHERE  bai.acct_no = LPAD(a.tmacctno,
--                                                 14,
--                                                 '0')) /*AND    TRUNC(TO_DATE(a.tmresv07, 'yyyyMMddHH24miss')) =
--                                                                                                                                                                                                                                                                                                                                                             TRUNC(SYSDATE)*/
--               --AND    TRIM(a.tmtxstat) = 'CE' --giao dich huy
--               -- AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
--               ) src
--        ON (c.tran_sn = src.tran_sn AND c.rollout_account_no = LPAD(src.tmacctno, 14, '0') AND c.amount = src.tmtxamt)
--        WHEN MATCHED THEN
--          UPDATE
--          SET    c.status  = DECODE(src.tmtxstat,
--                                    'CE',
--                                    'FAIL',
--                                    'SUCC'),
--                 c.core_sn = src.tmtxseq;
--        COMMIT;

        MERGE INTO bb_transfer_history c
        USING (SELECT b.tran_sn,
                      TRIM(a.tmtxstat) AS tmtxstat,
                      a.tmentdt7,
                      a.tmresv07,
                      a.tmacctno,
                      a.tmtxseq,
                      a.tmtxamt
               FROM   sync_tmtrans a,
                      sync_tranmap b
               WHERE  TRIM(a.tmtellid) = b.teller_id(+)
               AND    TRIM(a.tmtxseq) = b.host_tran_sn(+)
               AND    TRUNC(TO_DATE(a.tmentdt7,
                                    'yyyyddd')) =
                      TRUNC(TO_DATE(b.host_real_date(+),
                                     'yyyyddd'))
                     /*AND    LPAD(a.tmacctno,
                          14,
                          '0') IN
                     (SELECT bai.acct_no
                       FROM   bk_account_info bai
                       WHERE  bai.cif_no IN
                              (SELECT cif_no
                               FROM   bc_user_info))*/
               AND    EXISTS
                (SELECT bai.acct_no
                       FROM   sync_account_info bai
                       WHERE  bai.acct_no = (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end)) /*AND    TRUNC(TO_DATE(a.tmresv07, 'yyyyMMddHH24miss')) =
                                                                                                                                                                                                                                                                                                                                              TRUNC(SYSDATE)*/
               --AND    TRIM(a.tmtxstat) = 'CE' --giao dich huy
               -- AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
               ) src
        ON (c.tran_sn = src.tran_sn AND c.rollout_acct_no = (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end) AND c.amount = src.tmtxamt)
        WHEN MATCHED THEN
          UPDATE
          SET    c.status  = DECODE(src.tmtxstat,
                                    'CE',
                                    'FAIL',
                                    'SUCC'),
                 c.core_sn = src.tmtxseq;
        COMMIT;

        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_merge_bc;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;


    g_error_level := 4;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_merge_checkpoint;

        MERGE INTO sync_checkpoint a
        USING (SELECT MAX(TO_DATE(sd.tmresv07,
                                  'YYYYMMDDHH24MISS')) end_time
               FROM   sync_tmtrans sd /* WHERE  LENGTH(rtrim(sd.tmresv07)) = 14
                                                                                                                                                                                                                                                                                                                        AND    LENGTH(rtrim(sd.tmresv07)) IS NOT NULL
                                                                                                                                                                                                                                                                                                                        WHERE    TO_DATE(sd.tmresv07,
                                                                                                                                                                                                                                                                                                                                                       'yyyyMMddHH24miss') > v_checkpoint_time
                                                                                                                                                                                                                                                                                                                                    -- (SELECT DISTINCT a.sync_end_time
                                                                                                                                                                                                                                                                                                                                    --FROM   sync_checkpoint a
                                                                                                                                                                                                                                                                                                                                    --WHERE  a.sync_type = 'TMTRAN')
                                                                                                                                                                                                                                                                                                                        AND    TRUNC(TO_DATE(sd.tmresv07,
                                                                                                                                                                                                                                                                                                                                                                   'yyyyMMddHH24miss')) = TRUNC(SYSDATE)*/
               ) src
        ON (a.sync_type = 'TMTRAN' AND src.end_time IS NOT NULL)
        WHEN MATCHED THEN
          UPDATE
          SET    a.sync_end_time = src.end_time,
                 a.is_sync       = 'Y';

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_merge_checkpoint;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    --EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_tmtran_onday_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_tmtran_onday_sync',
                                    -- 'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;


  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync transaction history(on day) ko dung check point
  --ManhNV
  PROCEDURE proc_tmtran_onday_fail_sync IS
    v_checkpoint_number NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    v_is_sync         := 'N';
    g_loop_count      := 10;

    g_error_level := 1;

    SELECT TO_NUMBER(TO_CHAR(a.sync_end_time,
                             'yyyy') || TO_CHAR(a.sync_end_time,
                                                'mm') ||
                     TO_CHAR(a.sync_end_time,
                             'dd') || TO_CHAR(a.sync_end_time,
                                              'hh24') ||
                     TO_CHAR(a.sync_end_time,
                             'mi') || TO_CHAR(a.sync_end_time,
                                              'ss'))
    INTO   v_checkpoint_number
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'TMTRAN_FAIL';

    g_error_level := 2;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_temp;

        INSERT INTO sync_tmtrans_fail
          SELECT /*+ ALL_ROWS */
           trim(a.tmtxcd),
           a.tmresv07,
           a.tmdorc,
           a.tmtxamt,
           a.tmglcur,
           a.tmorgamt,
           a.tmtellid,
           a.tmefth,
           a.tmacctno,
           SYSDATE, --tminsdate,
           a.tmtellid,
           a.tmtxseq,
           a.tmtxstat,
           a.tmhosttxcd,
           a.tmapptype,
           a.tmeqvtrn,
           a.tmibttrn,
           a.tmsumtrn,
           '1', --tmtype
           'SUCC', --tmsts,
           a.tmentdt7,
           --'TM' || seq_core_sn_tm.nextval
           a.tmeffdt7,
           NULL,
           a.tmsseq,
           a.tmtiment
          FROM   STG.SI_DAT_TMTRAN@STAGING_PRO a
          WHERE  a.tmtxstat = 'CE' --giao dich huy
          AND    a.tmapptype <> 'G'
          AND    RTRIM(a.tmresv07) IS NOT NULL
          AND    TO_NUMBER(a.tmresv07) >= v_checkpoint_number
          AND    (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
          AND    (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
          AND    (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
          AND    a.tmhosttxcd NOT IN (77,
                                      129,
                                      178,
                                      179,
                                      185)
                --AND    (a.tmtxstat IS NULL OR a.tmtxstat NOT IN ('CE'))
          AND    a.tmapptype IS NOT NULL
          AND    a.tmdorc IN ('D',
                              'C');

        /*AND    LENGTH(rtrim(a.tmresv07)) = 14
        AND    LENGTH(rtrim(a.tmresv07)) IS NOT NULL*/
        --AND    a.tmtxstat NOT IN ('PP','PT');
        /*AND    trunc(to_date(a.tmentdt7, 'yyyyddd')) = trunc(SYSDATE);*/
        -- AND    TRUNC(TO_DATE(b.message_date,
        --'yyyymmdd')) = TRUNC(SYSDATE)
        -- AND    TO_DATE(a.tmresv07,
        --'YYYYMMDDHH24MISS') > v_checkpoint_time
        /* AND    TO_DATE(a.tmresv07,
                                     'yyyyMMddHH24miss') > v_checkpoint_time
        AND    TRUNC(TO_DATE(a.tmresv07,
                                                 'yyyyMMddHH24miss')) = TRUNC(SYSDATE) */
        --; --1201
        --FROM   svdatpv51.TMTRAN@DBLINK_DATA a;
        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;


    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_tranmap;

        INSERT INTO sync_tranmap
          SELECT /*+ ALL_ROWS */
           a.tran_sn,
           a.teller_id,
           a.host_tran_sn,
           a.host_real_date,
           a.sender_id
          FROM   bec.bec_msglog@dblink_tranmap a
          --FROM   bec.bec_msglog2 a
          WHERE  a.sorn = 'Y'
          AND    TRUNC(TO_DATE(a.message_date,
                               'yyyymmddhh24mi')) >= TRUNC(SYSDATE)
          AND    LENGTH(a.tran_sn) < 21;

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_tranmap;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 8;

    FOR j IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_bulk_merge;

        --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
        MERGE INTO bk_account_history c
        USING (SELECT distinct /*+ INDEX(sync_tmtrans, IDX_SYNC_TMTRAN) */
               --(TRIM(a.tmtellid) || TRIM(a.tmtxseq) || TRIM(a.tmentdt7)),
               --a.core_sn,
               --('TM' || seq_core_sn_tm.nextval) AS core_sn1,
                TO_DATE((a.tmentdt7 || ':' ||
                        LPAD(a.tmtiment,
                              6,
                              '0')),
                        'yyyyddd:hh24miss') AS post_date,
                TO_DATE((a.tmeffdt7 || ':' ||
                        LPAD(a.tmtiment,
                              6,
                              '0')),
                        'yyyyddd:hh24miss') AS tran_date,
                /*DECODE(length(TRIM(TMEFFDT7)), 7, TO_DATE(TMEFFDT7, 'yyyyddd'),
                             NULL) AS tran_date,
                DECODE(length(TRIM(tmentdt7)), 7, TO_DATE(tmentdt7, 'yyyyddd'),
                             NULL) AS post_date,*/
                TRIM(a.tmdorc) AS tmdorc,
                a.tmtxamt,
                TRIM(a.tmglcur) AS tmglcur,
                a.tmorgamt,
                /* DECODE(b.sender_id,
                NULL,
                'CNT',
                b.sender_id) AS channel_id, */
                --DECODE(e.staff_name, NULL, 'CNT', 'IBS'),
                --TRIM(a.tmefth) AS tmefth,
                SUBSTR(a.tmefth,
                       11,
                       LENGTH(a.tmefth)) AS tmefth,
                a.tmacctno,
                SYSDATE AS tminsdate,
                TRIM(a.tmtellid) AS tmtellid,
                a.tmtxseq,
                /*'1',
                'SUCC',
                'SYNC', */
                b.tran_sn,
                TRIM(a.tmtxstat) AS tmtxstat,
                a.tmentdt7,
                a.tmresv07,
                d.tran_service_code,
                --e.staff_name,
                a.tmsseq,
                a.tmhosttxcd,
                b.sender_id,
                a.tmtxcd -- anhnt6
               FROM   sync_tmtrans_fail a,
                      sync_tranmap      b,
                      sync_tran_code    d /*,
                                                                                                                                                                                                                                                                                                                                                              sync_teller       e*/
               WHERE  TRIM(a.tmtellid) = b.teller_id(+)
               AND    TRIM(a.tmtxseq) = b.host_tran_sn(+)
               AND    TRUNC(TO_DATE(a.tmentdt7,
                                    'yyyyddd')) =
                      TRUNC(TO_DATE(b.host_real_date(+),
                                     'yyyyddd'))
                     /* AND    EXISTS
                     (SELECT
                                     b.cif_no
                                    FROM   bk_account_info b
                                    WHERE  EXISTS (SELECT 1
                                                    FROM   bk_cif bc
                                                    WHERE  bc.cif_no = b.cif_no)) */
                     /*AND    LPAD(a.tmacctno,
                          14,
                          '0') IN
                     (SELECT bai.acct_no
                       FROM   bk_account_info bai
                       WHERE  bai.cif_no IN
                              (SELECT cif_no
                               FROM   bc_user_info))*/
               AND    EXISTS
                (SELECT bai.acct_no
                       FROM   bk_account_info bai
                       WHERE  bai.acct_no = (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end)
                       AND    bai.cif_no IN
                              (
                                SELECT cif_no
                                FROM   bb_corp_info))
               AND    TRIM(a.tmtxcd) = d.tran_code(+) --AND    TRIM(a.tmtellid) = e.staff_name(+)
               --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
               /* AND    (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
               AND    (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
               AND    (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
               AND    a.tmapptype NOT IN ('S',
                                                                      'G')
               AND    a.tmhosttxcd NOT IN (77,
                                                                       129,
                                                                       178,
                                                                       179,
                                                                       185)
                           --AND    (a.tmtxstat IS NULL OR a.tmtxstat NOT IN ('CE'))
               AND    a.tmtxstat <> 'CE' --giao dich huy
               AND    a.tmapptype IS NOT NULL
               AND    a.tmdorc IN ('D',
                                                       'C')
               AND    LENGTH(rtrim(a.tmresv07)) = 14
               AND    LENGTH(rtrim(a.tmresv07)) IS NOT NULL
                           -- AND    TRUNC(TO_DATE(b.message_date,
                           --'yyyymmdd')) = TRUNC(SYSDATE)
                           -- AND    TO_DATE(a.tmresv07,
                           --'YYYYMMDDHH24MISS') > v_checkpoint_time
               AND    TO_DATE(a.tmresv07,
                                              'yyyyMMddHH24miss') > v_checkpoint_time
               AND    TRUNC(TO_DATE(a.tmresv07,
                                                          'yyyyMMddHH24miss')) = TRUNC(SYSDATE) */
               ) src
        ON (c.teller_id = src.tmtellid AND c.tm_seq = src.tmtxseq AND TRUNC(c.post_time) = TRUNC(src.post_date) 
        AND c.rollout_acct_no = (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end) AND c.dc_sign = TRIM(src.tmdorc) AND c.tran_device = TRIM(src.tmsseq) AND c.device_no = TRIM(src.tmhosttxcd))
        WHEN MATCHED THEN
          UPDATE
          SET    c.status            = DECODE(src.tmtxstat,
                                              'CE',
                                              'FAIL',
                                              'SUCC'),
                 c.tran_service_code = DECODE(src.tran_service_code,
                                              NULL,
                                              'RTR001',
                                              'RTR002'),
                 /* DECODE(src.tmtxstat,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             'SUCC') */
                 --c.tran_sn           = src.tran_sn,
                 /*c.channel = DECODE(src.staff_name,
                 NULL,
                 'CNT',
                 'IBS'),*/
                 c.channel = DECODE(src.sender_id,
                                    NULL,
                                    'CNT',
                                    src.sender_id),
                 c.remark  = src.tmefth --,
          --c.tran_device = TRIM(src.tmsseq),
          --c.device_no = TRIM(src.tmhosttxcd)

















        WHEN NOT MATCHED THEN
          INSERT
            (c.core_sn,
             c.tran_time,
             c.post_time,
             c.dc_sign,
             c.amount,
             c.currency_code,
             c.pre_balance,
             c.channel,
             c.remark,
             c.rollout_acct_no,
             c.insert_date,
             c.teller_id,
             c.tm_seq,
             c.sync_type,
             c.status,
             c.tc_code,
             c.tran_sn,
             c.tran_service_code,
             c.tran_device,
             c.device_no,
             c.trace_code -- anhnt6
             )
          VALUES
            ( --src.core_sn1,
             TRIM(src.tmresv07) || seq_core_sn.NEXTVAL,
             --TRIM(src.tmdorc) || TRIM(src.tmtellid) || TRIM(src.tmtxseq) ||
             --TRIM(src.tmentdt7),
             src.tran_date,
             src.post_date,
             src.tmdorc,
             src.tmtxamt,
             src.tmglcur,
             src.tmorgamt,
             --src.channel_id,
             /*DECODE(src.staff_name,
             NULL,
             'CNT',
             'IBS'),*/
             DECODE(src.sender_id,
                    NULL,
                    'CNT',
                    src.sender_id),
             src.tmefth,
             (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end),
             src.tminsdate,
             src.tmtellid,
             src.tmtxseq,
             '0',
             DECODE(src.tmtxstat,
                    'CE',
                    'FAIL',
                    'SUCC'),
             --'SUCC'
             /* DECODE(src.tmtxstat,
             '',
             'FAIL',
             'PP',
             'PEND',
             'PT',
             'SUCC',
             'SUCC') ,*/
             'SYNC',
             src.tran_sn,
             DECODE(src.tran_service_code,
                    NULL,
                    'RTR001',
                    'RTR002'),
             TRIM(src.tmsseq),
             TRIM(src.tmhosttxcd),
             src.tmtxcd -- anhnt6
             );

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_bulk_merge;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 9;

    FOR j IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_merge_bc;

        --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
--        MERGE INTO bc_transfer_history c
--        USING (SELECT b.tran_sn,
--                      TRIM(a.tmtxstat) AS tmtxstat,
--                      a.tmentdt7,
--                      a.tmresv07,
--                      a.tmacctno,
--                      a.tmtxseq,
--                      a.tmtxamt
--               FROM   sync_tmtrans_fail a,
--                      sync_tranmap      b
--               WHERE  TRIM(a.tmtellid) = b.teller_id(+)
--               AND    TRIM(a.tmtxseq) = b.host_tran_sn(+)
--               AND    TRUNC(TO_DATE(a.tmentdt7,
--                                    'yyyyddd')) =
--                      TRUNC(TO_DATE(b.host_real_date(+),
--                                     'yyyyddd'))
--                     /*AND    LPAD(a.tmacctno,
--                          14,
--                          '0') IN
--                     (SELECT bai.acct_no
--                       FROM   bk_account_info bai
--                       WHERE  bai.cif_no IN
--                              (SELECT cif_no
--                               FROM   bc_user_info))*/
--               AND    EXISTS
--                (SELECT bai.acct_no
--                       FROM   bk_account_info bai
--                       WHERE  bai.acct_no = LPAD(a.tmacctno,
--                                                 14,
--                                                 '0')
--                       AND    bai.cif_no IN
--                              (SELECT cif_no
--                                FROM   bc_user_info))
--                     /*AND    TRUNC(TO_DATE(a.tmresv07, 'yyyyMMddHH24miss')) =
--                     TRUNC(SYSDATE)*/
--               AND    TRIM(a.tmtxstat) = 'CE' --giao dich huy
--               -- AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
--               ) src
--        ON (c.tran_sn = src.tran_sn AND c.rollout_account_no = LPAD(src.tmacctno, 14, '0') AND c.amount = src.tmtxamt)
--        WHEN MATCHED THEN
--          UPDATE
--          SET    c.status  = DECODE(src.tmtxstat,
--                                    'CE',
--                                    'FAIL',
--                                    'SUCC'),
--                 c.core_sn = src.tmtxseq;
--        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_merge_bc;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 10;

    FOR j IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_merge_bb;

        --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
        MERGE INTO bb_transfer_history c
        USING (SELECT b.tran_sn,
                      TRIM(a.tmtxstat) AS tmtxstat,
                      a.tmentdt7,
                      a.tmresv07,
                      a.tmacctno,
                      a.tmtxseq,
                      a.tmtxamt
               FROM   sync_tmtrans_fail a,
                      sync_tranmap      b
               WHERE  TRIM(a.tmtellid) = b.teller_id(+)
               AND    TRIM(a.tmtxseq) = b.host_tran_sn(+)
               AND    TRUNC(TO_DATE(a.tmentdt7,
                                    'yyyyddd')) =
                      TRUNC(TO_DATE(b.host_real_date(+),
                                     'yyyyddd'))
                     /*AND    LPAD(a.tmacctno,
                          14,
                          '0') IN
                     (SELECT bai.acct_no
                       FROM   bk_account_info bai
                       WHERE  bai.cif_no IN
                              (SELECT cif_no
                               FROM   bc_user_info))*/
               AND    EXISTS
                (SELECT bai.acct_no
                       FROM   bk_account_info bai
                       WHERE  bai.acct_no = (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end)
                       AND    bai.cif_no IN
                              (SELECT cif_no
                                FROM   bb_corp_info))
                     /*AND    TRUNC(TO_DATE(a.tmresv07, 'yyyyMMddHH24miss')) =
                     TRUNC(SYSDATE)*/
               AND    TRIM(a.tmtxstat) = 'CE' --giao dich huy
               -- AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
               ) src
        ON (c.tran_sn = src.tran_sn AND c.rollout_acct_no = (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end) AND c.amount = src.tmtxamt)
        WHEN MATCHED THEN
          UPDATE
          SET    c.status  = DECODE(src.tmtxstat,
                                    'CE',
                                    'FAIL',
                                    'SUCC'),
                 c.core_sn = src.tmtxseq;
        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_merge_bb;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;


    MERGE INTO sync_checkpoint a
    USING (SELECT MAX(TO_DATE(sd.tmresv07,
                              'YYYYMMDDHH24MISS')) end_time
           FROM   sync_tmtrans_fail sd /* WHERE  LENGTH(rtrim(sd.tmresv07)) = 14
                                                                                                                                                                                                                                                 AND    LENGTH(rtrim(sd.tmresv07)) IS NOT NULL
                                                                                                                                                                                                                                                 WHERE    TO_DATE(sd.tmresv07,
                                                                                                                                                                                                                                                                                'yyyyMMddHH24miss') > v_checkpoint_time
                                                                                                                                                                                                                                                             -- (SELECT DISTINCT a.sync_end_time
                                                                                                                                                                                                                                                             --FROM   sync_checkpoint a
                                                                                                                                                                                                                                                             --WHERE  a.sync_type = 'TMTRAN')
                                                                                                                                                                                                                                                 AND    TRUNC(TO_DATE(sd.tmresv07,
                                                                                                                                                                                                                                                                                            'yyyyMMddHH24miss')) = TRUNC(SYSDATE)*/
           ) src
    ON (a.sync_type = 'TMTRAN_FAIL' AND src.end_time IS NOT NULL)
    WHEN MATCHED THEN
      UPDATE
      SET    a.sync_end_time = src.end_time,
             a.is_sync       = 'Y';

    COMMIT;

    --EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_tmtran_onday_fail_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_tmtran_onday_fail_sync',
                                    -- 'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  --Lay tat ca giao dich trong bang TMTRAN
  PROCEDURE proc_tmtran_cif_sync IS
    v_checkpoint_date NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    v_is_sync         := 'N';
    g_loop_count      := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    /*SELECT   TO_NUMBER (
               TO_CHAR (SYSDATE, 'yyyy') || TO_CHAR (SYSDATE, 'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;*/

    SELECT TO_NUMBER(TO_CHAR(a.sync_end_time,
                             'yyyy') || TO_CHAR(a.sync_end_time,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'COREDATE';

    g_error_level := 3;

    INSERT INTO sync_cif_n
      SELECT cif_no,
             status
      FROM   sync_cif
      WHERE  status = 'N'
      AND    TYPE = 'TM';

    g_error_level := 4;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (SELECT cif_no
                             FROM   sync_cif_n));

    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_tmtrans
              SELECT /*+ ALL_ROWS */
               trim(a.tmtxcd),
               a.tmresv07,
               a.tmdorc,
               a.tmtxamt,
               a.tmglcur,
               a.tmorgamt,
               a.tmtellid,
               a.tmefth,
               a.tmacctno,
               SYSDATE, --tminsdate,
               a.tmtellid,
               a.tmtxseq,
               a.tmtxstat,
               a.tmhosttxcd,
               a.tmapptype,
               a.tmeqvtrn,
               a.tmibttrn,
               a.tmsumtrn,
               '1', --tmtype
               'SUCC', --tmsts,
               a.tmentdt7,
               --'TM' || seq_core_sn_tm.nextval
               a.tmeffdt7,
               NULL,
               a.tmsseq,
               a.tmtiment
              FROM   STG.SI_DAT_TMTRAN@STAGING_PRO a
              WHERE  (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
              AND    (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
              AND    (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
              AND    a.tmapptype NOT IN ('S',
                                         'G')
              AND    a.tmhosttxcd NOT IN (77,
                                          129,
                                          178,
                                          179,
                                          185)
                    --AND    a.tmtxstat <> 'CE' --giao dich huy
              AND    a.tmapptype IS NOT NULL
              AND    a.tmdorc IN ('D',
                                  'C')
              AND    LENGTH(RTRIM(a.tmresv07)) = 14
              AND    LENGTH(RTRIM(a.tmresv07)) IS NOT NULL
              AND    a.tmentdt7 >= (v_checkpoint_date - 1)
              AND (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end) IN (SELECT acct_no
                                    FROM   sync_account_info)
              AND    a.tmtiment BETWEEN g_min_time AND g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 2;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) >= TRUNC(SYSDATE - 1)
      AND    LENGTH(a.tran_sn) < 21;

    --AND    TRUNC(TO_DATE(a.host_real_date,'yyyyddd')) = TRUNC(SYSDATE);


    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            MERGE INTO   bk_account_history c
                 USING   (SELECT /*+ INDEX(sync_tmtrans, IDX_SYNC_TMTRAN) */
                                TO_DATE (
                                       (   a.tmentdt7
                                        || ':'
                                        || LPAD (a.tmtiment, 6, '0')),
                                       'yyyyddd:hh24miss')
                                       AS post_date,
                                   TO_DATE (
                                       (   a.tmeffdt7
                                        || ':'
                                        || LPAD (a.tmtiment, 6, '0')),
                                       'yyyyddd:hh24miss')
                                       AS tran_date,
                                   /*DECODE(length(TRIM(TMEFFDT7)), 7, TO_DATE(TMEFFDT7, 'yyyyddd'),
                                          NULL) AS tran_date,
                                   DECODE(length(TRIM(tmentdt7)), 7, TO_DATE(tmentdt7, 'yyyyddd'),
                                          NULL) AS post_date,*/
                                   TRIM (a.tmdorc) AS tmdorc,
                                   a.tmtxamt,
                                   TRIM (a.tmglcur) AS tmglcur,
                                   a.tmorgamt,
                                   DECODE (b.sender_id,
                                           NULL, 'CNT',
                                           b.sender_id)
                                       AS channel_id,
                                   --TRIM(a.tmefth) AS tmefth,
                                   SUBSTR (a.tmefth,
                                           11,
                                           LENGTH (a.tmefth))
                                       AS tmefth,
                                   a.tmacctno,
                                   SYSDATE AS tminsdate,
                                   TRIM (a.tmtellid) AS tmtellid,
                                   a.tmtxseq,
                                   /*'1',
                                   'SUCC',
                                   'SYNC', */
                                   b.tran_sn,
                                   a.tmtxstat,
                                   a.tmentdt7,
                                   a.tmresv07,
                                   d.tran_service_code,
                                   e.staff_name,
                                   a.tmhosttxcd,
                                   a.tmsseq,
                                   b.sender_id,
                                   TRIM (a.tmapptype) AS tmapptype,
                                   a.tmtxcd
                            FROM   sync_tmtrans a,
                                   sync_tranmap b,
                                   sync_tran_code d,
                                   sync_teller e
                           WHERE   TRIM (a.tmtellid) = b.teller_id(+)
                                   AND TRIM (a.tmtxseq) =
                                          b.host_tran_sn(+)
                                   AND TRUNC(TO_DATE (a.tmentdt7,
                                                      'yyyyddd')) =
                                          TRUNC(TO_DATE (
                                                    b.host_real_date(+),
                                                    'yyyyddd'))
                                   AND (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end) IN
                                              (SELECT   x.acct_no
                                                 FROM   sync_account_info x)
                                   AND TRIM (a.tmtxcd) = d.tran_code(+)
                                   AND TRIM (a.tmtellid) =
                                          e.staff_name(+)
                                   AND a.tmtiment BETWEEN g_min_time
                                                      AND  g_max_time)
                         src
                    ON   (    c.teller_id = src.tmtellid
                          AND c.tm_seq = src.tmtxseq
                          AND TRUNC (c.post_time) = TRUNC (src.post_date)
                          AND c.rollout_acct_no =
                          (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end)
                          AND c.dc_sign = TRIM (src.tmdorc)
                          AND c.tran_device = TRIM (src.tmsseq)
                          AND c.device_no = TRIM (src.tmhosttxcd))
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.status = 'SUCC' /* DECODE(src.tmtxstat,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              '',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'FAIL',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'PP',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'PEND',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'PT',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'SUCC',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'SUCC') */
                                     ,
                    c.tran_service_code =
                        DECODE (src.tran_service_code,
                                NULL, 'RTR001',
                                'RTR002'),
                    --c.tran_sn           = src.tran_sn,
                    /*c.channel = DECODE(src.staff_name,
                    NULL,
                    'CNT',
                    'IBS')*/
                    c.channel =
                        DECODE (src.sender_id,
                                NULL, 'CNT',
                                src.sender_id)
            WHEN NOT MATCHED
            THEN
                INSERT              (c.core_sn,
                                     c.tran_time,
                                     c.post_time,
                                     c.dc_sign,
                                     c.amount,
                                     c.currency_code,
                                     c.pre_balance,
                                     c.channel,
                                     c.remark,
                                     c.rollout_acct_no,
                                     c.insert_date,
                                     c.teller_id,
                                     c.tm_seq,
                                     c.sync_type,
                                     c.status,
                                     c.tc_code,
                                     c.tran_sn,
                                     c.tran_service_code,
                                     c.trace_code --anhnt6
                                     )
                    VALUES   (                             --src.core_sn1,
                              TRIM (src.tmresv07) || seq_core_sn.NEXTVAL,
                              --TRIM(src.tmdorc) || TRIM(src.tmtellid) || TRIM(src.tmtxseq) ||
                              --TRIM(src.tmentdt7),
                              src.tran_date,
                              src.post_date,
                              src.tmdorc,
                              src.tmtxamt,
                              src.tmglcur,
                              src.tmorgamt,
                              --src.channel_id,
                              /*DECODE(src.staff_name,
                              NULL,
                              'CNT',
                              'IBS')*/
                              DECODE (src.sender_id,
                                      NULL, 'CNT',
                                      src.sender_id),
                              src.tmefth,
                              (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end),
                              src.tminsdate,
                              src.tmtellid,
                              src.tmtxseq,
                              --'0',
                              DECODE (src.tmapptype,
                                      'D', '6',
                                      'T', '7',
                                      'L', '8',
                                      'S', '6',
                                      '0'),
                              'SUCC' /* DECODE(src.tmtxstat,
                                     '',
                                     'FAIL',
                                     'PP',
                                     'PEND',
                                     'PT',
                                     'SUCC',
                                     'SUCC') */
                                    ,
                              'SYNC',
                              src.tran_sn,
                              DECODE (src.tran_service_code,
                                      NULL, 'RTR001',
                                      'RTR002'),
                                      src.tmtxcd);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat END

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_merge_bc;

            g_max_time := g_min_time + g_limit_time;

--            MERGE INTO   bc_transfer_history c
--                 USING   (SELECT   b.tran_sn,
--                                   TRIM (a.tmtxstat) AS tmtxstat,
--                                   a.tmentdt7,
--                                   a.tmresv07,
--                                   a.tmacctno
--                            FROM   sync_tmtrans a, sync_tranmap b
--                           WHERE   TRIM (a.tmtellid) = b.teller_id(+)
--                                   AND TRIM (a.tmtxseq) =
--                                          b.host_tran_sn(+)
--                                   AND TRUNC(TO_DATE (a.tmentdt7,
--                                                      'yyyyddd')) =
--                                          TRUNC(TO_DATE (
--                                                    b.host_real_date(+),
--                                                    'yyyyddd'))
--                                   AND LPAD (a.tmacctno, 14, '0') IN
--                                              (SELECT   x.acct_no
--                                                 FROM   sync_account_info x)
--                                   AND a.tmtiment BETWEEN g_min_time
--                                                      AND  g_max_time /*AND    TRUNC(TO_DATE(a.tmresv07, 'yyyyMMddHH24miss')) =
--                                                                                                  TRUNC(SYSDATE)*/
--                                                                     --AND    TRIM(a.tmtxstat) = 'CE' --giao dich huy
--                                                                     -- AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
--                         ) src
--                    ON   (c.tran_sn = src.tran_sn
--                          AND c.rollout_account_no =
--                                 LPAD (src.tmacctno, 14, '0'))
--            WHEN MATCHED
--            THEN
--                UPDATE SET
--                    c.status = DECODE (src.tmtxstat, 'CE', 'FAIL', 'SUCC');
--            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    /* DELETE FROM sync_cif
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    and    type = 'TM'; */
    UPDATE sync_cif
    SET    status = 'Y'
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n scn1)
    AND    "TYPE" = 'TM';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_tmtran_cif_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_TMTRAN_NEW_CIF');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_TMTRAN_NEW_CIF');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_tmtran_cif_sync',
                                    'SYSTEM BUSY',
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) ,*/
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync transaction history(on day) ko dung check point
  --ManhNV
  PROCEDURE proc_tmtran_hist_sync IS
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    v_is_sync         := 'N';
    g_loop_count      := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 3;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info));

    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_tmtrans
              SELECT /*+ ALL_ROWS */
               trim(a.tmtxcd),
               a.tmresv07,
               a.tmdorc,
               a.tmtxamt,
               a.tmglcur,
               a.tmorgamt,
               a.tmtellid,
               a.tmefth,
               a.tmacctno,
               SYSDATE, --tminsdate,
               a.tmtellid,
               a.tmtxseq,
               a.tmtxstat,
               a.tmhosttxcd,
               a.tmapptype,
               a.tmeqvtrn,
               a.tmibttrn,
               a.tmsumtrn,
               '1', --tmtype
               'SUCC', --tmsts,
               a.tmentdt7,
               --'TM' || seq_core_sn_tm.nextval
               a.tmeffdt7,
               NULL,
               a.tmsseq,
               a.tmtiment
              FROM   STG.SI_DAT_TMTRAN@STAGING_PRO a
              WHERE  (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
              AND    (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
              AND    (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
              AND    a.tmapptype NOT IN ('S',
                                         'G')
              AND    a.tmhosttxcd NOT IN (77,
                                          129,
                                          178,
                                          179,
                                          185)
                    --AND    (a.tmtxstat IS NULL OR a.tmtxstat NOT IN ('CE'))
                    --AND    a.tmtxstat <> 'CE' --giao dich huy
              AND    a.tmapptype IS NOT NULL
              AND    a.tmdorc IN ('D',
                                  'C')
              AND    LENGTH(RTRIM(a.tmresv07)) = 14
              AND    LENGTH(RTRIM(a.tmresv07)) IS NOT NULL
              AND    TRUNC(TO_DATE(a.tmentdt7,
                                   'yyyyddd')) >= TRUNC(SYSDATE - 2)
              AND   (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end) IN (SELECT acct_no
                                    FROM   sync_account_info)
              AND    a.tmtiment BETWEEN g_min_time AND g_max_time;

            /* AND    TRUNC(TO_DATE(b.message_date,
            'yyyymmdd')) = TRUNC(SYSDATE) */
            /* AND    TO_DATE(a.tmresv07,
            'YYYYMMDDHH24MISS') > v_checkpoint_time */
            /* AND    TRUNC(TO_DATE(a.tmresv07,
            'yyyyMMddHH24miss')) = TRUNC(p_tran_date); */
            --AND    TRIM(tmapptype) = p_tran_type; --1201
            --FROM   svdatpv51.TMTRAN@DBLINK_DATA a;
            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 2;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) >= TRUNC(SYSDATE - 2)
      AND    LENGTH(a.tran_sn) < 21;

    --AND    TRUNC(TO_DATE(a.host_real_date,'yyyyddd')) = TRUNC(SYSDATE);

    /*MERGE INTO sync_checkpoint a
    USING (SELECT MAX(TO_DATE(sd.tmresv07, 'YYYYMMDDHH24MISS')) end_time
           FROM   sync_tmtrans sd
           \* WHERE  LENGTH(rtrim(sd.tmresv07)) = 14
           AND    LENGTH(rtrim(sd.tmresv07)) IS NOT NULL
           AND    TRUNC(TO_DATE(sd.tmresv07,
                                'yyyyMMddHH24miss')) > TRUNC(p_tran_date)
           AND    TRIM(sd.tmapptype) = p_tran_type *\
           ) src
    ON (a.sync_type = DECODE(p_tran_type, 'D', 'DDHIST', 'T', 'FDHIST', 'L', 'LNHIST') AND src.end_time IS NOT NULL)
    WHEN MATCHED THEN
      UPDATE
      SET    a.sync_end_time = src.end_time,
             a.is_sync       = 'Y';*/
    /* WHEN NOT MATCHED THEN
    INSERT
      (a.id,
       a.sync_type,
       a.sync_end_time,
       a.is_sync)
    VALUES
      (seq_sync_checkpoint.nextval,
       'TMTRAN',
       src.end_time,
       'Y'); */

    --xoa cac giao dich cua ngay hom truoc vaf thuc hien insert lai

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            MERGE INTO   bk_account_history c
                 USING   (SELECT /*+ INDEX(sync_tmtrans, IDX_SYNC_TMTRAN) */
                                   --(TRIM(a.tmtellid) || TRIM(a.tmtxseq) || TRIM(a.tmentdt7)),
                                   --a.core_sn,
                                   --('TM' || seq_core_sn_tm.nextval) AS core_sn1,
                                   TO_DATE (
                                       (   a.tmentdt7
                                        || ':'
                                        || LPAD (a.tmtiment, 6, '0')),
                                       'yyyyddd:hh24miss')
                                       AS post_date,
                                   TO_DATE (
                                       (   a.tmeffdt7
                                        || ':'
                                        || LPAD (a.tmtiment, 6, '0')),
                                       'yyyyddd:hh24miss')
                                       AS tran_date,
                                   /*DECODE(length(TRIM(TMEFFDT7)), 7, TO_DATE(TMEFFDT7, 'yyyyddd'),
                                          NULL) AS tran_date,
                                   DECODE(length(TRIM(tmentdt7)), 7, TO_DATE(tmentdt7, 'yyyyddd'),
                                          NULL) AS post_date,*/
                                   TRIM (a.tmdorc) AS tmdorc,
                                   a.tmtxamt,
                                   TRIM (a.tmglcur) AS tmglcur,
                                   a.tmorgamt,
                                   /* DECODE(b.sender_id,
                                   NULL,
                                   'CNT',
                                   b.sender_id) AS channel_id, */
                                   --DECODE(e.staff_name, NULL, 'CNT', 'IBS'),
                                   --TRIM(a.tmefth) AS tmefth,
                                   SUBSTR (a.tmefth,
                                           11,
                                           LENGTH (a.tmefth))
                                       AS tmefth,
                                   a.tmacctno,
                                   SYSDATE AS tminsdate,
                                   TRIM (a.tmtellid) AS tmtellid,
                                   a.tmtxseq,
                                   /*'1',
                                   'SUCC',
                                   'SYNC', */
                                   b.tran_sn,
                                   TRIM (a.tmtxstat) AS tmtxstat,
                                   a.tmentdt7,
                                   a.tmresv07,
                                   d.tran_service_code,
                                   e.staff_name,
                                   a.tmsseq,
                                   a.tmhosttxcd,
                                   b.sender_id,
                                   TRIM (a.tmapptype) AS tmapptype,
                                   a.tmtxcd --anhnt6
                            FROM   sync_tmtrans a,
                                   sync_tranmap b,
                                   sync_tran_code d,
                                   sync_teller e
                           WHERE   TRIM (a.tmtellid) = b.teller_id(+)
                                   AND TRIM (a.tmtxseq) =
                                          b.host_tran_sn(+)
                                   AND TRUNC(TO_DATE (a.tmentdt7,
                                                      'yyyyddd')) =
                                          TRUNC(TO_DATE (
                                                    b.host_real_date(+),
                                                    'yyyyddd'))
                                   /* AND    EXISTS
                                   (SELECT
                                           b.cif_no
                                          FROM   bk_account_info b
                                          WHERE  EXISTS (SELECT 1
                                                  FROM   bk_cif bc
                                                  WHERE  bc.cif_no = b.cif_no)) */
                                   AND (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end) IN
                                              (SELECT   x.acct_no
                                                 FROM   sync_account_info x)
                                   --to_date('14/11/2011 18:16:53','dd/mm/yyyy hh24:mi:ss')--v_checkpoint_time
                                   /* AND    TRUNC(TO_DATE(a.tmresv07,
                                   'yyyyMMddHH24miss')) = TRUNC(SYSDATE) */
                                   AND TRIM (a.tmtxcd) = d.tran_code(+)
                                   AND TRIM (a.tmtellid) =
                                          e.staff_name(+)
                                   AND a.tmtiment BETWEEN g_min_time
                                                      AND  g_max_time /* AND    (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
                                                                      AND    (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
                                                                      AND    (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
                                                                      AND    a.tmapptype NOT IN ('S',
                                                                                                 'G')
                                                                      AND    a.tmhosttxcd NOT IN (77,
                                                                                                  129,
                                                                                                  178,
                                                                                                  179,
                                                                                                  185)
                                                                            --AND    (a.tmtxstat IS NULL OR a.tmtxstat NOT IN ('CE'))
                                                                      AND    a.tmtxstat <> 'CE' --giao dich huy
                                                                      AND    a.tmapptype IS NOT NULL
                                                                      AND    a.tmdorc IN ('D',
                                                                                          'C')
                                                                      AND    LENGTH(rtrim(a.tmresv07)) = 14
                                                                      AND    LENGTH(rtrim(a.tmresv07)) IS NOT NULL
                                                                            -- AND    TRUNC(TO_DATE(b.message_date,
                                                                            --'yyyymmdd')) = TRUNC(SYSDATE)
                                                                            -- AND    TO_DATE(a.tmresv07,
                                                                            --'YYYYMMDDHH24MISS') > v_checkpoint_time
                                                                      AND    TO_DATE(a.tmresv07,
                                                                                     'yyyyMMddHH24miss') > v_checkpoint_time
                                                                      AND    TRUNC(TO_DATE(a.tmresv07,
                                                                                           'yyyyMMddHH24miss')) = TRUNC(SYSDATE) */
                                                                     ) src
                    ON   (    c.teller_id = src.tmtellid
                          AND c.tm_seq = src.tmtxseq
                          AND TRUNC (c.post_time) = TRUNC (src.post_date)
                          AND c.rollout_acct_no = (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end)
                          AND c.dc_sign = TRIM (src.tmdorc)
                          AND c.tran_device = TRIM (src.tmsseq)
                          AND c.device_no = TRIM (src.tmhosttxcd))
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.status = DECODE (src.tmtxstat, 'CE', 'FAIL', 'SUCC'),
                    c.tran_service_code =
                        DECODE (src.tran_service_code,
                                NULL, 'RTR001',
                                'RTR002'), /* DECODE(src.tmtxstat,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          '',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'FAIL',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'PP',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'PEND',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'PT',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'SUCC',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          'SUCC') */
                    --c.tran_sn           = src.tran_sn,--,
                    --c.tran_device = TRIM(src.tmsseq),
                    --c.device_no = TRIM(src.tmhosttxcd)
                    /*c.channel = DECODE(src.staff_name,
                    NULL,
                    'CNT',
                    'IBS'),*/
                    c.channel =
                        DECODE (src.sender_id,
                                NULL, 'CNT',
                                src.sender_id),
                    c.remark = src.tmefth
            WHEN NOT MATCHED
            THEN
                INSERT              (c.core_sn,
                                     c.tran_time,
                                     c.post_time,
                                     c.dc_sign,
                                     c.amount,
                                     c.currency_code,
                                     c.pre_balance,
                                     c.channel,
                                     c.remark,
                                     c.rollout_acct_no,
                                     c.insert_date,
                                     c.teller_id,
                                     c.tm_seq,
                                     c.sync_type,
                                     c.status,
                                     c.tc_code,
                                     c.tran_sn,
                                     c.tran_service_code,
                                     c.tran_device,
                                     c.device_no,
                                     c.trace_code)
                    VALUES   (                             --src.core_sn1,
                              TRIM (src.tmresv07) || seq_core_sn.NEXTVAL,
                              --TRIM(src.tmdorc) || TRIM(src.tmtellid) || TRIM(src.tmtxseq) ||
                              --TRIM(src.tmentdt7),
                              src.tran_date,
                              src.post_date,
                              src.tmdorc,
                              src.tmtxamt,
                              src.tmglcur,
                              src.tmorgamt,
                              --src.channel_id,
                              /*DECODE(src.staff_name,
                              NULL,
                              'CNT',
                              'IBS'),*/
                              DECODE (src.sender_id,
                                      NULL, 'CNT',
                                      src.sender_id),
                              src.tmefth,
                              (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end),
                              src.tminsdate,
                              src.tmtellid,
                              src.tmtxseq,
                              DECODE (src.tmapptype,
                                      'D', '6',
                                      'T', '7',
                                      'L', '8',
                                      'S', '6',
                                      '0'),
                              DECODE (src.tmtxstat, 'CE', 'FAIL', 'SUCC'),
                              --'SUCC'
                              /* DECODE(src.tmtxstat,
                              '',
                              'FAIL',
                              'PP',
                              'PEND',
                              'PT',
                              'SUCC',
                              'SUCC') ,*/
                              'SYNC',
                              src.tran_sn,
                              DECODE (src.tran_service_code,
                                      NULL, 'RTR001',
                                      'RTR002'),
                              TRIM (src.tmsseq),
                              TRIM (src.tmhosttxcd),
                              src.tmtxcd);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    g_error_level := 8;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_merge_bc;

            g_max_time := g_min_time + g_limit_time;

--            MERGE INTO   bc_transfer_history c
--                 USING   (SELECT   b.tran_sn,
--                                   TRIM (a.tmtxstat) AS tmtxstat,
--                                   a.tmentdt7,
--                                   a.tmresv07,
--                                   a.tmacctno
--                            FROM   sync_tmtrans a, sync_tranmap b
--                           WHERE   TRIM (a.tmtellid) = b.teller_id(+)
--                                   AND TRIM (a.tmtxseq) =
--                                          b.host_tran_sn(+)
--                                   AND TRUNC(TO_DATE (a.tmentdt7,
--                                                      'yyyyddd')) =
--                                          TRUNC(TO_DATE (
--                                                    b.host_real_date(+),
--                                                    'yyyyddd'))
--                                   AND LPAD (a.tmacctno, 14, '0') IN
--                                              (SELECT   x.acct_no
--                                                 FROM   sync_account_info x)
--                                   AND a.tmtiment BETWEEN g_min_time
--                                                      AND  g_max_time /*AND    TRUNC(TO_DATE(a.tmresv07,
--                                                                                                                'yyyyMMddHH24miss')) = TRUNC(SYSDATE - 1)*/
--                                                                     --AND    TRIM(a.tmtxstat) = 'CE' --giao dich huy
--                                                                     --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
--                         ) src
--                    ON   (c.tran_sn = src.tran_sn
--                          AND c.rollout_account_no =
--                                 LPAD (src.tmacctno, 14, '0'))
--            WHEN MATCHED
--            THEN
--                UPDATE SET
--                    c.status = DECODE (src.tmtxstat, 'CE', 'FAIL', 'SUCC');
--
--            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    g_error_level := 9;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    /*    MERGE INTO bk_account_history c
    USING (SELECT DECODE(length(TRIM(tmentdt7)),
                         7,
                         TO_DATE(tmentdt7,
                                 'yyyyddd'),
                         NULL) AS post_date,
                  a.tmacctno,
                  TRIM(a.tmtellid) AS tmtellid,
                  a.tmtxseq,
                  b.tran_sn,
                  TRIM(a.tmtxstat) AS tmtxstat,
                  a.tmentdt7,
                  a.tmsseq,
                  a.tmhosttxcd,
                  a.tmdorc
           FROM   sync_tmtrans a,
                  sync_tranmap b
           WHERE  TRIM(a.tmtellid) = b.teller_id(+)
           AND    TRIM(a.tmtxseq) = b.host_tran_sn(+)
           AND    TRUNC(TO_DATE(a.tmentdt7,
                                'yyyyddd')) =
                  TRUNC(TO_DATE(b.host_real_date(+),
                                 'yyyyddd'))
           AND    LPAD(a.tmacctno,
                       14,
                       '0') IN (SELECT x.acct_no
                                 FROM   sync_account_info x)
           AND    TRUNC(TO_DATE(a.tmresv07,
                                'yyyyMMddHH24miss')) = TRUNC(SYSDATE - 1)
           AND    TRIM(a.tmtxstat) = 'CE' --cac giao dich huy
           --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           ) src
    ON (c.teller_id = src.tmtellid AND c.tm_seq = src.tmtxseq AND TRUNC(c.post_time) = TRUNC(src.post_date) AND c.rollout_acct_no = LPAD(src.tmacctno, 14, '0') AND c.dc_sign = TRIM(src.tmdorc) AND c.tran_device = TRIM(src.tmsseq) AND c.device_no = TRIM(src.tmhosttxcd))
    WHEN MATCHED THEN
      UPDATE
      SET    c.status = DECODE(src.tmtxstat,
                               'CE',
                               'FAIL',
                               'SUCC');*/

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_tmtran_hist_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_TMTRAN_END_DAY');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_tmtran_hist_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_TMTRAN_END_DAY');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync salary(on day)
  -- ManhNV
  PROCEDURE proc_salary_onday_sync IS
    v_checkpoint_date NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;
    v_checkpoint_date := 0;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    SELECT TO_NUMBER(TO_CHAR(SYSDATE,
                             'yyyy') || TO_CHAR(SYSDATE,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    g_error_level := 1;

    --reset seq_core_sn
    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_SALARY.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_SALARY increment by -' ||
                      (l_val - p_min_value) || ' minvalue ' || p_min_value;

    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_SALARY.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_SALARY increment by 1 minvalue ' ||
                      p_min_value;

    g_error_level := 2;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_ddtrn2
              SELECT a.trancd,
                     a.trdate,
                     a.dorc,
                     a.amt,
                     a.trctyp,
                     a.trefth,
                     a.tracct,
                     --'SL' || seq_core_sn_salary.nextval,
                     NULL,
                     a.treffd,
                     a.trtime
              FROM   RAWSTAGEUAT.SI_DAT_DDTRN2@RAWSTAGE_PRO a
              WHERE  a.dorc IN ('D',
                                'C')
              AND    a.trdate = v_checkpoint_date
                    --AND    a.trtime BETWEEN g_min_time AND g_max_time
              AND    a.trtime >= g_min_time
              AND    a.trtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;

    SELECT /*+ ALL_ROWS */
     a.core_sn BULK COLLECT
    INTO   core_sn_tab
    FROM   bk_account_history a
    WHERE  TRUNC(post_time) = TRUNC(SYSDATE)
    AND    sync_type = 5; --salary;

    g_error_level := 3;

    FORALL i IN core_sn_tab.FIRST .. core_sn_tab.LAST
      DELETE bk_account_history a
      WHERE  a.core_sn = core_sn_tab(i); --salary;

    g_error_level := 7;

    INSERT INTO sync_account_info
      SELECT cif_no,
             acct_no
      FROM   bk_account_info a
      WHERE  a.cif_no IN (
                          SELECT cif_no
                          FROM   bb_corp_info);

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            post_time,
                                            tran_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            status,
                                            sync_type,
                                            tran_service_code)
                (SELECT         /*+ INDEX(sync_ddtrn2, IDX_SYNC_DDTRN2) */
                       'SL'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_salary.NEXTVAL,
                          TO_DATE (a.trdate, 'yyyyddd'),
                          DECODE (LENGTH (a.treffd),
                                  7, TO_DATE (a.treffd, 'yyyyddd'),
                                  NULL),
                          --a.TMTIMENT,
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          NULL,
                          'CNT',
                          --TRIM(a.trefth),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth))
                              AS trefth,
                              (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          'SUCC',
                          '5',                                    --salary
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002')
                   FROM   sync_ddtrn2 a, sync_tran_code d
                  WHERE   a.trtime >= g_min_time
                                        AND a.trtime < g_max_time
                        --a.trtime BETWEEN g_min_time AND g_max_time
                          AND TRIM (a.trancd) = d.tran_service_code(+)
                          --AND a.trancd IN ('164', '920') --164: salary, 920: memo credit
                          AND EXISTS
                                 (SELECT   bai.acct_no
                                    FROM   sync_account_info bai
                                   WHERE   bai.acct_no = (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end)
                                               ));

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_salary_onday_sync',
                                  NULL,
                                  'SUCC');
    --ebank_common_sync.proc_update_status_job_sync('Y', 'SYNC_SALARY');

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      --ebank_common_sync.proc_update_status_job_sync('N', 'SYNC_SALARY');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_salary_onday_sync',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*      DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync salary(on day)
  -- ManhNV
  -- Cai lay bo vi khong can thuc hien
  PROCEDURE proc_salary_cif_sync IS
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;

    /*--reset seq_core_sn
    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_SALARY.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_SALARY increment by -' ||
                      (l_val - p_min_value) || ' minvalue ' || p_min_value;

    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_SALARY.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_SALARY increment by 1 minvalue ' ||
                      p_min_value;*/

    g_error_level := 1;

    INSERT INTO sync_cif_n
      SELECT cif_no,
             status
      FROM   sync_cif
      WHERE  status = 'N'
      AND    TYPE = 'SL';

    g_error_level := 2;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (SELECT cif_no
                             FROM   sync_cif_n sct));

    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_temp;

        INSERT INTO sync_ddtrn2
          SELECT a.trancd,
                 a.trdate,
                 a.dorc,
                 a.amt,
                 a.trctyp,
                 a.trefth,
                 a.tracct,
                 --'SL' || seq_core_sn_salary.nextval,
                 NULL,
                 a.treffd,
                 a.trtime
          FROM   RAWSTAGEUAT.SI_DAT_DDTRN2@RAWSTAGE_PRO a
          WHERE  a.dorc IN ('D',
                            'C')
          AND    TRUNC(TO_DATE(a.trdate,
                               'yyyyddd')) >=
                 TRUNC(ADD_MONTHS(TRUNC(SYSDATE,
                                         'MM'),
                                   -6))
          AND    TRUNC(TO_DATE(a.trdate,
                               'yyyyddd')) < TRUNC(SYSDATE - 3);

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    DELETE FROM bk_account_history
    WHERE  rollout_acct_no IN
           (SELECT acct_no
            FROM   sync_account_info sai)
    AND    TRUNC(post_time) < TRUNC(SYSDATE - 3)
    AND    sync_type = 5; --salary;

    g_error_level := 7;

    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    INSERT INTO bk_account_history
      (core_sn,
       post_time,
       tran_time,
       dc_sign,
       amount,
       currency_code,
       pre_balance,
       channel,
       remark,
       rollout_acct_no,
       insert_date,
       status,
       sync_type,
       tran_service_code)
      (SELECT /*+ INDEX(sync_ddtrn2, IDX_SYNC_DDTRN2) */
        'SL' || TO_CHAR(a.trdate) || seq_core_sn_salary.NEXTVAL,
        TO_DATE(a.trdate,
                'yyyyddd'),
        TO_DATE(a.treffd,
                'yyyyddd'),
        --a.TMTIMENT,
        TRIM(a.dorc),
        a.amt,
        TRIM(a.trctyp),
        NULL,
        'CNT',
        --TRIM(a.trefth),
        SUBSTR(a.trefth,
               11,
               LENGTH(a.trefth)) AS trefth,
        a.tracct,
        SYSDATE,
        'SUCC',
        '5', --salary
        DECODE(d.tran_service_code,
               NULL,
               'RTR001',
               'RTR002')
       FROM   sync_ddtrn2    a,
              sync_tran_code d
       WHERE  TRIM(a.trancd) = d.tran_service_code(+)
       AND    a.trancd IN ('164',
                           '920') --164: salary, 920: memo credit
       AND  (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN (SELECT acct_no
                             FROM   sync_account_info) --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
       );

    UPDATE sync_cif
    SET    status = 'Y'
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    AND    "TYPE" = 'SL';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_salary_cif_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_SALARY_NEW_CIF');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_SALARY_NEW_CIF');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_salary_cif_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*      DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  -- Xoa lich su onday
  -- Do lich su luong tu ddhist
  -- Bo di, gop lan vao trancode_hist
  PROCEDURE proc_salary_hist_sync IS
  BEGIN
    v_start_date := SYSDATE;
    p_min_value  := 0;
    v_is_sync    := 'N';
    g_loop_count := 10;

    --Get all data from DDHIST core
    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_insert_temp;

        INSERT INTO sync_ddhist
          SELECT /*+ ALL_ROWS */
           a.trancd,
           a.trdat6,
           a.dorc,
           a.amt,
           a.trctyp,
           a.camt,
           a.trefth,
           a.tracct,
           a.trdate,
           --NULL,
           a.treffd,
           a.trtime,
           a.seq,
           TRIM(a.truser),
                     TRIM(a.auxtrc)
          --'DD' || SEQ_CORE_SN_DD.NEXTVAL
          FROM   STG.SI_HIS_DDHIST@STAGING_PRO a
          WHERE  TRUNC(TO_DATE(a.trdate,
                               'yyyyddd')) >= TRUNC(SYSDATE - 1)
          AND    a.dorc IN ('D',
                            'C')
          AND    a.trancd = '164';

        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    --delete all hist of DDHIST
    /* DELETE FROM bk_account_history a
    WHERE  (a.core_sn LIKE 'DD%' OR
           (a.teller_id IS NOT NULL AND a.tm_seq IS NOT NULL AND
           a.tran_time IS NOT NULL AND
           TRUNC(a.tran_time) <> TRUNC(SYSDATE))); */

    g_error_level := 2;

    DELETE FROM bk_account_history
    WHERE  sync_type = 5 --salary
    AND    TRUNC(post_time) >= TRUNC(SYSDATE - 1); --1:dd transaction

    g_error_level := 3;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info));

    /*g_error_level := 4;
    SELECT
    \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   min_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            FROM   sync_ddhist
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 1
    ORDER  BY 1;

    g_error_level := 5;
    SELECT
    \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   max_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            FROM   sync_ddhist
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 0
    UNION
    SELECT
    \*+ ALL_ROWS *\
     MAX(ROWID)
    FROM   sync_ddhist;*/

    g_error_level := 6;

    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    INSERT INTO bk_account_history
      (core_sn,
       tran_time,
       post_time,
       dc_sign,
       amount,
       currency_code,
       pre_balance,
       channel,
       remark,
       rollout_acct_no,
       insert_date,
       sync_type,
       status,
       tran_service_code,
       trace_code -- anhnt6
       )
      (SELECT /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
       --a.trancd,
       --a.core_sn,
        'DD' || TO_CHAR(a.trdate) || seq_core_sn_salary.NEXTVAL,
        /*TO_DATE(a.trdate, 'yyyyddd'),
        TO_DATE(a.TREFFD, 'yyyyddd'),*/
        TO_DATE((a.trdate || ':' || LPAD(a.trtime,
                                         6,
                                         '0')),
                'yyyyddd:hh24miss'),
        TO_DATE((a.treffd || ':' || LPAD(a.trtime,
                                         6,
                                         '0')),
                'yyyyddd:hh24miss'),
        TRIM(a.dorc),
        a.amt,
        TRIM(a.trctyp),
        a.camt,
        'CNT',
        --TRIM(a.trefth),
        SUBSTR(a.trefth,
               11,
               LENGTH(a.trefth)),
        (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),       
        SYSDATE,
        '5', --dd transaction
        'SUCC',
        DECODE(d.tran_service_code,
               NULL,
               'RTR001',
               'RTR002'),
               a.auxtrc -- anhnt6
       FROM   sync_ddhist    a,
              sync_tran_code d
       /* WHERE  TRUNC(TO_DATE(a.trdate,
       'yyyyddd')) > TRUNC(p_checkpoint_time_ddhist) */
       WHERE  TRIM(a.auxtrc) = d.tran_service_code(+)
       AND  (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN (SELECT acct_no
                             FROM   sync_account_info) --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
       );

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_salary_cif_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_SALARY_END_DAY');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_SALARY_END_DAY');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_salary_cif_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_ddhist_cif_sync IS
    v_min_date NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_is_sync         := 'N';
    g_loop_count      := 10;
    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_min_date        := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 1;

    SELECT TO_NUMBER(TO_CHAR(SYSDATE - 1,
                             'yyyy') || TO_CHAR(SYSDATE - 1,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    SELECT TO_NUMBER(TO_CHAR(TRUNC(ADD_MONTHS(TRUNC(SYSDATE,
                                                    'MM'),
                                              -6)),
                             'yyyy') || TO_CHAR(TRUNC(ADD_MONTHS(TRUNC(SYSDATE,
                                                                       'MM'),
                                                                 -6)),
                                                'ddd'))
    INTO   v_min_date
    FROM   DUAL;

    INSERT INTO sync_cif_n
      SELECT cif_no,
             status
      FROM   sync_cif
      WHERE  status = 'N'
      AND    TYPE = 'DD';

    g_error_level := 2;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (SELECT cif_no
                             FROM   sync_cif_n sct)
       AND    bai.acct_type IN ('CA',
                                'SA','31'));

    --Get all data from DDHIST core
    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_ddhist
              SELECT /*+ ALL_ROWS */
               a.trancd,
               a.trdat6,
               a.dorc,
               a.amt,
               a.trctyp,
               a.camt,
               a.trefth,
               a.tracct,
               a.trdate,
               --NULL,
               a.treffd,
               a.trtime,
               a.seq,
               TRIM(a.truser),
                             TRIM(a.auxtrc)
              --'DD' || SEQ_CORE_SN_DD.NEXTVAL
              FROM   STG.SI_HIS_DDHIST@STAGING_PRO a
              WHERE  a.dorc IN ('D',
                                'C') --AND    a.trancd NOT IN('160','164','165')
              AND    a.trancd NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.trdate >= v_min_date
              AND    a.trdate < (v_checkpoint_date)
              AND    a.tracct IN (SELECT TO_NUMBER(x.acct_no)
                                  FROM   sync_account_info x)
                    --AND    a.trtime BETWEEN g_min_time AND g_max_time
              AND    a.trtime >= g_min_time
              AND    a.trtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    DELETE FROM bk_account_history
    WHERE  rollout_acct_no IN
           (SELECT acct_no
            FROM   sync_account_info sai)
    AND    TRUNC(post_time) < TRUNC(SYSDATE - 1)
    AND    (sync_type = 1 OR sync_type = 5 OR sync_type = 6);


    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) < TRUNC(SYSDATE - 1)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            trace_code -- anhnt6
                                                                            )
                (SELECT         /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
                             --a.trancd,
                             --a.core_sn,
                             'DD'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_dd.NEXTVAL,
                          /*TO_DATE(a.trdate, 'yyyyddd'),
                          TO_DATE(a.TREFFD, 'yyyyddd'),*/
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TO_DATE (
                              (a.treffd || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          a.camt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.trefth),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          '1',                            --dd transaction
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.truser,
                          a.seq,
                                        c.tran_sn,
                                        TRIM(a.auxtrc) -- anhnt6
                   FROM   sync_ddhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.auxtrc) = d.tran_service_code(+)
                          AND TRIM (a.truser) = c.teller_id(+)
                          AND TRIM (a.seq) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.trdate, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.truser)) = b.staff_name(+)
                          AND (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN
                                     (SELECT   x.acct_no
                                        FROM   sync_account_info x)
                          --AND a.trtime BETWEEN g_min_time AND g_max_time
                                        AND a.trtime >= g_min_time
                                        AND a.trtime < g_max_time
                                        );
            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    /* DELETE FROM sync_cif
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    and    type = 'DD'; */

    UPDATE sync_cif
    SET    status = 'Y'
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    AND    "TYPE" = 'DD';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddhist_cif_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_DDHIST_NEW_CIF');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_DDHIST_NEW_CIF');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddhist_cif_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;


  /*
    Nhung loai giao dich cu chuoi, chi thuc hien vao cuoi ngay,
    do luon vao ddhist
  */
  PROCEDURE proc_ddhist_sync IS
    v_count NUMBER;
  BEGIN
    v_start_date := SYSDATE;
    p_min_value  := 0;
    v_is_sync    := 'N';
    g_loop_count := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 3;

    SELECT TO_NUMBER(TO_CHAR(SYSDATE - 1,
                             'yyyy') || TO_CHAR(SYSDATE - 1,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
       AND    bai.acct_type IN ('CA',
                                'SA','31'));

    --Get all data from DDHIST core
    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_ddhist
              SELECT /*+ ALL_ROWS */
               a.trancd,
               a.trdat6,
               a.dorc,
               a.amt,
               a.trctyp,
               a.camt,
               a.trefth,
               a.tracct,
               a.trdate,
               --NULL,
               a.treffd,
               a.trtime,
               a.seq,
               TRIM(a.truser),
                             TRIM(a.AUXTRC)
              --'DD' || SEQ_CORE_SN_DD.NEXTVAL
              FROM   STG.SI_HIS_DDHIST@STAGING_PRO a
              WHERE  a.trdate = (v_checkpoint_date)
              AND    a.dorc IN ('D',
                                'C')
              AND    a.trancd NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.tracct IN (SELECT to_number(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.trtime BETWEEN g_min_time AND g_max_time
              AND    a.trtime >= g_min_time
              AND    a.trtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 2;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_ddhist;

    --Check dam bao buoc 1 chay thanh cong, va co du lieu trong sync_ddhist
    --Dam bao cho truong hop job chay truoc khi chay batch
    BEGIN

      IF (v_count > 0)
      THEN
        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
          AND    TRUNC(post_time) = TRUNC(SYSDATE - 1);

        /*DELETE FROM bk_account_history
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction*/

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE;
    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) >= TRUNC(SYSDATE - 1)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 6;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            DELETE FROM bk_account_history
            WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
            AND    TRUNC(post_time) = TRUNC(SYSDATE - 1)
            AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) >= g_min_time
                        AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) < g_max_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            trace_code --anhnt6
                                                                            )
                (SELECT         /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
                             --a.trancd,
                             --a.core_sn,
                             'DD'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_dd.NEXTVAL,
                          /*TO_DATE(a.trdate, 'yyyyddd'),
                          TO_DATE(a.TREFFD, 'yyyyddd'),*/
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TO_DATE (
                              (a.treffd || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          a.camt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.trefth),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          '6',                            --dd transaction
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.truser,
                          a.seq,
                                        c.tran_sn,
                                        trim(a.auxtrc) -- anhnt6
                   FROM   sync_ddhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  /* WHERE  TRUNC(TO_DATE(a.trdate,
                  'yyyyddd')) > TRUNC(p_checkpoint_time_ddhist) */
                  WHERE       TRIM (a.auxtrc) = d.tran_service_code(+)
                          AND TRIM (a.truser) = c.teller_id(+)
                          AND TRIM (a.seq) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.trdate, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.truser)) = b.staff_name(+)
                          AND (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.trtime BETWEEN g_min_time AND g_max_time
                                        AND a.trtime >= g_min_time
                                        AND a.trtime < g_max_time);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time        := g_min_time - g_limit_time;
                g_max_time        := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    COMMIT;

    --noformat end

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddhist_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_DDHIST_END_DAY');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_DDHIST_END_DAY');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddhist_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  /*
    Nhung loai giao dich cu chuoi, chi thuc hien vao cuoi ngay,
    do luon vao ddhist
  */
  PROCEDURE proc_ddhist_by_date_acct_sync
  (
    p_acct_no VARCHAR2,
    p_date    DATE
  ) IS
    v_count NUMBER;
  BEGIN
    v_start_date := SYSDATE;
    p_min_value  := 0;
    v_is_sync    := 'N';
    g_loop_count := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 3;

    SELECT TO_NUMBER(TO_CHAR(p_date,
                             'yyyy') || TO_CHAR(p_date,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.acct_no = p_acct_no);

    --Get all data from DDHIST core
    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_ddhist
              SELECT /*+ ALL_ROWS */
               a.trancd,
               a.trdat6,
               a.dorc,
               a.amt,
               a.trctyp,
               a.camt,
               a.trefth,
               a.tracct,
               a.trdate,
               --NULL,
               a.treffd,
               a.trtime,
               a.seq,
               TRIM(a.truser),
                             TRIM(a.auxtrc)
              --'DD' || SEQ_CORE_SN_DD.NEXTVAL
              FROM   STG.SI_HIS_DDHIST@STAGING_PRO a
              WHERE  a.trdate = (v_checkpoint_date)
              AND    a.dorc IN ('D',
                                'C')
              AND    a.trancd NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.tracct IN (SELECT to_number(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.trtime BETWEEN g_min_time AND g_max_time
              AND    a.trtime >= g_min_time
              AND    a.trtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 2;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_ddhist;

    --Check dam bao buoc 1 chay thanh cong, va co du lieu trong sync_ddhist
    --Dam bao cho truong hop job chay truoc khi chay batch
    BEGIN

      IF (v_count > 0)
      THEN
        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no = p_acct_no; --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
          AND    TRUNC(post_time) = TRUNC(p_date)
          AND    rollout_acct_no = p_acct_no;

        /*DELETE FROM bk_account_history
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no = p_acct_no; --1:dd transaction*/

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE;
    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) = TRUNC(p_date)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 6;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            DELETE FROM bk_account_history
            WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
            AND    TRUNC(post_time) = TRUNC(p_date)
            AND    rollout_acct_no = p_acct_no
            AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) >= g_min_time
                        AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) < g_max_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            trace_code
                                                                            )
                (SELECT         /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
                             --a.trancd,
                             --a.core_sn,
                             'DD'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_dd.NEXTVAL,
                          /*TO_DATE(a.trdate, 'yyyyddd'),
                          TO_DATE(a.TREFFD, 'yyyyddd'),*/
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TO_DATE (
                              (a.treffd || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          a.camt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.trefth),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          '6',                            --dd transaction
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.truser,
                          a.seq,
                                        c.tran_sn,
                                        a.auxtrc -- anhnt6
                   FROM   sync_ddhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  /* WHERE  TRUNC(TO_DATE(a.trdate,
                  'yyyyddd')) > TRUNC(p_checkpoint_time_ddhist) */
                  WHERE       TRIM (a.auxtrc) = d.tran_service_code(+)
                          AND TRIM (a.truser) = c.teller_id(+)
                          AND TRIM (a.seq) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.trdate, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.truser)) = b.staff_name(+)
                          AND (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.trtime BETWEEN g_min_time AND g_max_time
                                        AND a.trtime >= g_min_time
                                        AND a.trtime < g_max_time);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time        := g_min_time - g_limit_time;
                g_max_time        := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    COMMIT;

    --noformat end

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddhist_manual_by_date_account_sync',
                                  NULL,
                                  'SUCC');

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddhist_manual_by_date_account_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_ddhist_by_date_sync(p_date DATE) IS
    v_count NUMBER;
  BEGIN
    v_start_date := SYSDATE;
    p_min_value  := 0;
    v_is_sync    := 'N';
    g_loop_count := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 3;

    SELECT TO_NUMBER(TO_CHAR(p_date,
                             'yyyy') || TO_CHAR(p_date,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
       AND    bai.acct_type IN ('CA',
                                'SA','31'));

    --Get all data from DDHIST core
    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_ddhist
              SELECT /*+ ALL_ROWS */
               a.trancd,
               a.trdat6,
               a.dorc,
               a.amt,
               a.trctyp,
               a.camt,
               a.trefth,
               a.tracct,
               a.trdate,
               --NULL,
               a.treffd,
               a.trtime,
               a.seq,
               TRIM(a.truser),
                             TRIM(a.auxtrc)
              --'DD' || SEQ_CORE_SN_DD.NEXTVAL
              FROM   STG.SI_HIS_DDHIST@STAGING_PRO a
              WHERE  a.trdate = (v_checkpoint_date)
              AND    a.dorc IN ('D',
                                'C')
              AND    a.trancd NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.tracct IN (SELECT to_number(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.trtime BETWEEN g_min_time AND g_max_time
              AND    a.trtime >= g_min_time
              AND    a.trtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 2;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_ddhist;

    --Check dam bao buoc 1 chay thanh cong, va co du lieu trong sync_ddhist
    --Dam bao cho truong hop job chay truoc khi chay batch
    BEGIN

      IF (v_count > 0)
      THEN
        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no IN
               (SELECT acct_no
                 FROM   sync_account_info); --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
          AND    TRUNC(post_time) = TRUNC(p_date)
          AND    rollout_acct_no IN
                 (SELECT acct_no
                   FROM   sync_account_info);


      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE;
    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) = TRUNC(p_date)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 6;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            DELETE FROM bk_account_history
            WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
            AND    TRUNC(post_time) = TRUNC(p_date)
            AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) >= g_min_time
                        AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) < g_max_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            trace_code
                                                                            )
                (SELECT         /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
                             --a.trancd,
                             --a.core_sn,
                             'DD'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_dd.NEXTVAL,
                          /*TO_DATE(a.trdate, 'yyyyddd'),
                          TO_DATE(a.TREFFD, 'yyyyddd'),*/
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TO_DATE (
                              (a.treffd || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          a.camt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.trefth),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          '6',                            --dd transaction
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.truser,
                          a.seq,
                                        c.tran_sn,
                                        trim(a.auxtrc) -- anhnt6
                   FROM   sync_ddhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  /* WHERE  TRUNC(TO_DATE(a.trdate,
                  'yyyyddd')) > TRUNC(p_checkpoint_time_ddhist) */
                  WHERE       TRIM (a.auxtrc) = d.tran_service_code(+)
                          AND TRIM (a.truser) = c.teller_id(+)
                          AND TRIM (a.seq) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.trdate, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.truser)) = b.staff_name(+)
                          AND (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.trtime BETWEEN g_min_time AND g_max_time
                                        AND a.trtime >= g_min_time
                                        AND a.trtime < g_max_time);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time        := g_min_time - g_limit_time;
                g_max_time        := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    COMMIT;

    --noformat end

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddhist_manual_by_date_sync',
                                  NULL,
                                  'SUCC');

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddhist_manual_by_date_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  /*
    Nhung loai giao dich cu chuoi, chi thuc hien vao cuoi ngay,
    do luon vao ddhist
  */
  PROCEDURE proc_ddhist_trancode_sync IS
  BEGIN
    v_start_date := SYSDATE;
    p_min_value  := 0;
    v_is_sync    := 'N';
    g_loop_count := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 3;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info));

    --Get all data from DDHIST core
    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_ddhist
              SELECT /*+ ALL_ROWS */
               a.trancd,
               a.trdat6,
               a.dorc,
               a.amt,
               a.trctyp,
               a.camt,
               a.trefth,
               a.tracct,
               a.trdate,
               --NULL,
               a.treffd,
               a.trtime,
               a.seq,
               TRIM(a.truser),
                             TRIM(a.auxtrc)
              --'DD' || SEQ_CORE_SN_DD.NEXTVAL
              FROM   STG.SI_HIS_DDHIST@STAGING_PRO a
              WHERE  TRUNC(TO_DATE(a.trdate,
                                   'yyyyddd')) >= TRUNC(SYSDATE - 1)
              AND    a.dorc IN ('D',
                                'C')
              AND    a.trancd IN ('160',
                                  '164',
                                  '165',
                                  '146',
                                  '362')
                    /*AND LPAD (a.tracct, 14, '0') IN
                    (SELECT   acct_no
                       FROM   sync_account_info)*/
              AND    a.trtime BETWEEN g_min_time AND g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;


    --delete all hist of DDHIST
    /* DELETE FROM bk_account_history a
    WHERE  (a.core_sn LIKE 'DD%' OR
           (a.teller_id IS NOT NULL AND a.tm_seq IS NOT NULL AND
           a.tran_time IS NOT NULL AND
           TRUNC(a.tran_time) <> TRUNC(SYSDATE))); */

    g_error_level := 2;

    DELETE FROM bk_account_history
    WHERE  sync_type = 5 --4:salary, 5:other code
    AND    TRUNC(post_time) >= TRUNC(SYSDATE - 1); --1:dd transaction

    g_error_level := 6;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            trace_code
                                            )
                (SELECT         /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
                             --a.trancd,
                             --a.core_sn,
                             'TC'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_tc.NEXTVAL,
                          /*TO_DATE(a.trdate, 'yyyyddd'),
                          TO_DATE(a.TREFFD, 'yyyyddd'),*/
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TO_DATE (
                              (a.treffd || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          a.camt,
                          'CNT',
                          --TRIM(a.trefth),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          '5',                            --dd transaction
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                                  a.auxtrc -- anhnt6
                   FROM   sync_ddhist a, sync_tran_code d
                  /* WHERE  TRUNC(TO_DATE(a.trdate,
                  'yyyyddd')) > TRUNC(p_checkpoint_time_ddhist) */
                  WHERE   TRIM (a.auxtrc) = d.tran_service_code(+)
                          AND (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          AND a.trtime BETWEEN g_min_time AND g_max_time);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddhist_trancode_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_TRANCODE_END_DAY');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_TRANCODE_END_DAY');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddhist_trancode_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  --Cai nay bo vi gop lan vao ddhist
  PROCEDURE proc_ddhist_trancode_cif_sync IS
  BEGIN
    v_start_date := SYSDATE;
    p_min_value  := 0;
    v_is_sync    := 'N';
    g_loop_count := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 3;

    INSERT INTO sync_cif_n
      SELECT cif_no,
             status
      FROM   sync_cif
      WHERE  status = 'N'
      AND    TYPE = 'TC';

    g_error_level := 4;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (SELECT cif_no
                             FROM   sync_cif_n));

    --Get all data from DDHIST core
    g_error_level := 1;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_ddhist
              SELECT /*+ ALL_ROWS */
               a.trancd,
               a.trdat6,
               a.dorc,
               a.amt,
               a.trctyp,
               a.camt,
               a.trefth,
               a.tracct,
               a.trdate,
               --NULL,
               a.treffd,
               a.trtime,
               a.seq,
               TRIM(a.truser),
                             TRIM(a.auxtrc)
              --'DD' || SEQ_CORE_SN_DD.NEXTVAL
              FROM   STG.SI_HIS_DDHIST@STAGING_PRO a
              WHERE  TRUNC(TO_DATE(a.trdate,
                                   'yyyyddd')) >= TRUNC(SYSDATE - 2)
              AND    a.dorc IN ('D',
                                'C')
              AND    a.trancd IN ('160',
                                  '165')
                    /*AND LPAD (a.tracct, 14, '0') IN
                    (SELECT   acct_no
                       FROM   sync_account_info)*/
              AND    a.trtime BETWEEN g_min_time AND g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    --delete all hist of DDHIST
    /* DELETE FROM bk_account_history a
    WHERE  (a.core_sn LIKE 'DD%' OR
           (a.teller_id IS NOT NULL AND a.tm_seq IS NOT NULL AND
           a.tran_time IS NOT NULL AND
           TRUNC(a.tran_time) <> TRUNC(SYSDATE))); */

    g_error_level := 2;

    DELETE FROM bk_account_history
    WHERE  sync_type = 4 --other trancode
    AND    TRUNC(post_time) >= TRUNC(SYSDATE - 2); --1:dd transaction

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            trace_code)
                (SELECT         /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
                             --a.trancd,
                             --a.core_sn,
                             'DD'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_dd.NEXTVAL,
                          /*TO_DATE(a.trdate, 'yyyyddd'),
                          TO_DATE(a.TREFFD, 'yyyyddd'),*/
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TO_DATE (
                              (a.treffd || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          a.camt,
                          'CNT',
                          --TRIM(a.trefth),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          '4',                            --dd transaction
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                                  a.auxtrc -- anhnt6
                   FROM   sync_ddhist a, sync_tran_code d
                  /* WHERE  TRUNC(TO_DATE(a.trdate,
                  'yyyyddd')) > TRUNC(p_checkpoint_time_ddhist) */
                  WHERE   TRIM (a.auxtrc) = d.tran_service_code(+)
                          AND (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          AND a.trtime BETWEEN g_min_time AND g_max_time);
            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    UPDATE sync_cif
    SET    status = 'Y'
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n scn1)
    AND    "TYPE" = 'TC';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_ddhist_trancode_cif_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_TRANCODE_NEW_CIF');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('Y',
                                                    'SYNC_TRANCODE_NEW_CIF');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddhist_trancode_cif_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_cdhist_sync IS
    v_count NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;
    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_error_level := 2;

    SELECT TO_NUMBER(TO_CHAR(SYSDATE - 1,
                             'yyyy') || TO_CHAR(SYSDATE - 1,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
       AND    bai.acct_type IN ('FD')
       AND    bai.p_acct_no IS NOT NULL);

    g_error_level := 3;

    g_min_time   := 0;
    g_max_time   := 0;
    g_limit_time := 60000;
    g_time_count := 240000;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_cdhist
              SELECT /*+ ALL_ROWS */
               a.chtran,
               a.chdorc,
               a.chamt,
               a.chcurr,
               a.chcamt,
               a.chvarf,
               a.chacct,
               a.chefdt,
               --NULL,    --'FD' || seq_core_sn_fd.nextval
               a.chpstd,
               a.chseqn,
               TRIM(a.chuser),
               a.chtime
              FROM   svhispv51.cdhist@dblink_data a
              WHERE  a.chdorc IN ('D',
                                  'C')
              AND a.chafft in ('B', 'C') --anhnt6
              AND    a.chpstd = (v_checkpoint_date)
              AND    a.chtran NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.chacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.chtime BETWEEN g_min_time AND g_max_time
              AND    a.chtime >= g_min_time
              AND    a.chtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_cdhist;

    BEGIN
      IF (v_count > 0)
      THEN

        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
          AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        /*DELETE FROM bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction*/

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE;
    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) >= TRUNC(SYSDATE - 1)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

                        DELETE FROM bk_account_history
            WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
            AND    TRUNC(post_time) = TRUNC(SYSDATE - 1)
                        AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) >= g_min_time
                        AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) < g_max_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn)
                (SELECT         /*+ INDEX(sync_cdhist, IDX_SYNC_CDHIST) */
                             --a.chtran,
                             --a.core_sn,
                             'FD'
                          || TO_CHAR (a.chpstd)
                          || seq_core_sn_fd.NEXTVAL,
                          TO_DATE (a.chefdt, 'yyyyddd'),
                          TO_DATE (a.chpstd, 'yyyyddd'),
                          TRIM (a.chdorc),
                          a.chamt,
                          TRIM (a.chcurr),
                          a.chcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.chvarf),
                          SUBSTR (a.chvarf, 11, LENGTH (a.chvarf)),
                          (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end),
                          SYSDATE,
                          '7',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.chuser,
                          a.chseqn,
                                        c.tran_sn
                   FROM   sync_cdhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.chtran) = d.tran_service_code(+)
                          AND TRIM (a.chuser) = c.teller_id(+)
                          AND TRIM (a.chseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.chpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.chuser)) = b.staff_name(+)
                          AND (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.chtime BETWEEN g_min_time AND g_max_time
                                        AND a.chtime >= g_min_time
                                        AND a.chtime < g_max_time);


            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                    g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdhist_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_CDHIST_END_DAY');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_CDHIST_END_DAY');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdhist_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_cdhist_by_date_acct_sync
  (
    p_acct_no VARCHAR2,
    p_date    DATE
  ) IS
    v_count NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;
    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_error_level := 2;

    SELECT TO_NUMBER(TO_CHAR(p_date,
                             'yyyy') || TO_CHAR(p_date,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.acct_no = p_acct_no);

    g_error_level := 3;

    g_min_time   := 0;
    g_max_time   := 0;
    g_limit_time := 60000;
    g_time_count := 240000;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_cdhist
              SELECT /*+ ALL_ROWS */
               a.chtran,
               a.chdorc,
               a.chamt,
               a.chcurr,
               a.chcamt,
               a.chvarf,
               a.chacct,
               a.chefdt,
               --NULL,    --'FD' || seq_core_sn_fd.nextval
               a.chpstd,
               a.chseqn,
               TRIM(a.chuser),
               a.chtime
              FROM   svhispv51.cdhist@dblink_data a
              WHERE  a.chdorc IN ('D',
                                  'C')
              AND a.chafft in ('B', 'C') --anhnt6
              AND    a.chpstd = (v_checkpoint_date)
              AND    a.chtran NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.chacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.chtime BETWEEN g_min_time AND g_max_time
              AND    a.chtime >= g_min_time
              AND    a.chtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_cdhist;

    BEGIN
      IF (v_count > 0)
      THEN

        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no = p_acct_no; --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
          AND    TRUNC(post_time) = TRUNC(p_date)
          AND    rollout_acct_no = p_acct_no; --1:dd transaction

        DELETE FROM bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no = p_acct_no; --1:dd transaction

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE;
    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) = TRUNC(p_date)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn)
                (SELECT         /*+ INDEX(sync_cdhist, IDX_SYNC_CDHIST) */
                             --a.chtran,
                             --a.core_sn,
                             'FD'
                          || TO_CHAR (a.chpstd)
                          || seq_core_sn_fd.NEXTVAL,
                          TO_DATE (a.chefdt, 'yyyyddd'),
                          TO_DATE (a.chpstd, 'yyyyddd'),
                          TRIM (a.chdorc),
                          a.chamt,
                          TRIM (a.chcurr),
                          a.chcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.chvarf),
                          SUBSTR (a.chvarf, 11, LENGTH (a.chvarf)),
                          (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end),
                          SYSDATE,
                          '7',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.chuser,
                          a.chseqn,
                                        c.tran_sn
                   FROM   sync_cdhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.chtran) = d.tran_service_code(+)
                          AND TRIM (a.chuser) = c.teller_id(+)
                          AND TRIM (a.chseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.chpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.chuser)) = b.staff_name(+)
                          AND (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.chtime BETWEEN g_min_time AND g_max_time
                                        AND a.chtime >= g_min_time
                                        AND a.chtime < g_max_time);


            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                    g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdhist_by_date_acct_sync',
                                  NULL,
                                  'SUCC');

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdhist_by_date_acct_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_cdhist_by_date_sync(p_date DATE) IS
    v_count NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;
    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_error_level := 2;

    SELECT TO_NUMBER(TO_CHAR(p_date,
                             'yyyy') || TO_CHAR(p_date,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
       AND    bai.acct_type IN ('FD')
       AND    bai.p_acct_no IS NOT NULL);

    g_error_level := 3;

    g_min_time   := 0;
    g_max_time   := 0;
    g_limit_time := 60000;
    g_time_count := 240000;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_cdhist
              SELECT /*+ ALL_ROWS */
               a.chtran,
               a.chdorc,
               a.chamt,
               a.chcurr,
               a.chcamt,
               a.chvarf,
               a.chacct,
               a.chefdt,
               --NULL,    --'FD' || seq_core_sn_fd.nextval
               a.chpstd,
               a.chseqn,
               TRIM(a.chuser),
               a.chtime
              FROM   svhispv51.cdhist@dblink_data a
              WHERE  a.chdorc IN ('D',
                                  'C')
              AND a.chafft in ('B', 'C') --anhnt6
              AND    a.chpstd = (v_checkpoint_date)
              AND    a.chtran NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.chacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.chtime BETWEEN g_min_time AND g_max_time
              AND    a.chtime >= g_min_time
              AND    a.chtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_cdhist;

    BEGIN
      IF (v_count > 0)
      THEN

        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no IN
               (SELECT acct_no
                 FROM   sync_account_info); --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
          AND    TRUNC(post_time) = TRUNC(p_date)
          AND    rollout_acct_no IN
                 (SELECT acct_no
                   FROM   sync_account_info); --1:dd transaction

        DELETE FROM bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no IN
               (SELECT acct_no
                 FROM   sync_account_info); --1:dd transaction

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        RAISE;
    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) = TRUNC(p_date)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn)
                (SELECT         /*+ INDEX(sync_cdhist, IDX_SYNC_CDHIST) */
                             --a.chtran,
                             --a.core_sn,
                             'FD'
                          || TO_CHAR (a.chpstd)
                          || seq_core_sn_fd.NEXTVAL,
                          TO_DATE (a.chefdt, 'yyyyddd'),
                          TO_DATE (a.chpstd, 'yyyyddd'),
                          TRIM (a.chdorc),
                          a.chamt,
                          TRIM (a.chcurr),
                          a.chcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.chvarf),
                          SUBSTR (a.chvarf, 11, LENGTH (a.chvarf)),
                          (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end),
                          SYSDATE,
                          '7',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.chuser,
                          a.chseqn,
                                        c.tran_sn
                   FROM   sync_cdhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.chtran) = d.tran_service_code(+)
                          AND TRIM (a.chuser) = c.teller_id(+)
                          AND TRIM (a.chseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.chpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.chuser)) = b.staff_name(+)
                          AND (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.chtime BETWEEN g_min_time AND g_max_time
                                        AND a.chtime >= g_min_time
                                        AND a.chtime < g_max_time);


            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                    g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;

                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdhist_by_date_sync',
                                  NULL,
                                  'SUCC');

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdhist_by_date_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_cdhist_cif_sync IS
    v_min_date NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;
    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_sub_count := 0;
    g_error_count     := 0;
    v_checkpoint_date := 0;
    v_min_date        := 0;

    g_limit_time := 60000;
    g_time_count := 240000;


    g_error_level := 1;

    SELECT TO_NUMBER(TO_CHAR(SYSDATE - 1,
                             'yyyy') || TO_CHAR(SYSDATE - 1,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    SELECT TO_NUMBER(TO_CHAR(TRUNC(ADD_MONTHS(TRUNC(SYSDATE,
                                                    'MM'),
                                              -6)),
                             'yyyy') || TO_CHAR(TRUNC(ADD_MONTHS(TRUNC(SYSDATE,
                                                                       'MM'),
                                                                 -6)),
                                                'ddd'))
    INTO   v_min_date
    FROM   DUAL;

    INSERT INTO sync_cif_n
      SELECT cif_no,
             status
      FROM   sync_cif
      WHERE  status = 'N'
      AND    TYPE = 'CD';

    g_error_level := 2;

    INSERT INTO sync_account_info a
      (SELECT cif_no,
              acct_no
       FROM   bk_account_info
       WHERE  cif_no IN (SELECT cif_no
                         FROM   sync_cif_n sct)
       AND    acct_type IN ('FD')
       AND    p_acct_no IS NOT NULL);

    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_cdhist
              SELECT /*+ ALL_ROWS */
               a.chtran,
               a.chdorc,
               a.chamt,
               a.chcurr,
               a.chcamt,
               a.chvarf,
               a.chacct,
               a.chefdt,
               --NULL,    --'FD' || seq_core_sn_fd.nextval
               a.chpstd,
               a.chseqn,
               TRIM(a.chuser),
               a.chtime
              FROM   svhispv51.cdhist@dblink_data a
              WHERE  a.chdorc IN ('D',
                                  'C')
              AND a.chafft in ('B', 'C') --anhnt6
              AND    a.chtran NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
              AND    a.chpstd >= v_min_date
              AND    a.chpstd < (v_checkpoint_date)
              AND    a.chacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.chtime BETWEEN g_min_time AND g_max_time
              AND    a.chtime >= g_min_time
              AND    a.chtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    DELETE FROM bk_account_history
    WHERE  rollout_acct_no IN
           (SELECT acct_no
            FROM   sync_account_info sai)
    AND    TRUNC(post_time) < TRUNC(SYSDATE - 1)
    AND    (sync_type = 2 OR sync_type = 7) /*AND    sync_type NOT IN ('4','5')*/
    ;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) < TRUNC(SYSDATE - 1)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn)
                (SELECT         /*+ INDEX(sync_cdhist, IDX_SYNC_CDHIST) */
                             --a.chtran,
                             --a.core_sn,
                             'FD'
                          || TO_CHAR (a.chpstd)
                          || seq_core_sn_fd.NEXTVAL,
                          TO_DATE (a.chefdt, 'yyyyddd'),
                          TO_DATE (a.chpstd, 'yyyyddd'),
                          TRIM (a.chdorc),
                          a.chamt,
                          TRIM (a.chcurr),
                          a.chcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.chvarf),
                          SUBSTR (a.chvarf, 11, LENGTH (a.chvarf)),
                          (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end),
                          SYSDATE,
                          '2',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.chuser,
                          a.chseqn,
                                        c.tran_sn
                   FROM   sync_cdhist a, sync_tran_code d, --sync_teller b,
                                                          sync_tranmap c
                  WHERE       TRIM (a.chtran) = d.tran_service_code(+)
                          --AND TRIM (TRIM (a.chuser)) = b.staff_name(+)
                          AND TRIM (a.chuser) = c.teller_id(+)
                          AND TRIM (a.chseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.chpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          AND (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.chtime BETWEEN g_min_time AND g_max_time
                                        AND a.chtime >= g_min_time
                                        AND a.chtime < g_max_time);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    /* DELETE FROM sync_cif
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    and    type = 'CD'; */

    UPDATE sync_cif
    SET    status = 'Y'
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    AND    "TYPE" = 'CD';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cdhist_cif_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_CDHIST_NEW_CIF');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_CDHIST_NEW_CIF');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cdhist_cif_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_lnhist_sync IS
    v_count NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 2;

    SELECT TO_NUMBER(TO_CHAR(SYSDATE - 1,
                             'yyyy') || TO_CHAR(SYSDATE - 1,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
       AND    bai.acct_type IN ('LN'));


    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_lnhist
              SELECT /*+ ALL_ROWS */
               a.lhtran,
               a.lhpstd,
               a.lhefdt,
               a.lhdorc,
               a.lhamt,
               a.lhcur,
               a.lhcamt,
               a.lhtext,
               a.lhacct,
               --NULL,    --'LN' || SEQ_CORE_SN_LN.NEXTVAL
               a.lhseqn,
               TRIM(a.lhuser),
               a.lhtime,
               a.lhosbl, --anhnt6
               a.lhdudt -- anhnt6
              FROM   STG.SI_HIS_LNHIST@STAGING_PRO a
              WHERE  a.lhdorc IN ('D',
                                  'C')
              AND    a.lhpstd = (v_checkpoint_date)
              --AND    a.lhtran NOT IN (77,
              --                        129,
              --                        178, --179,
              --                        185)
              AND a.lhtran IN (912,993,990,914,922,915,121,101,962,974,988,42,41,976,35,15,30,62,61,23,22,21,145,143,497,496,781,889,926,102,811,812,906,43,964
              )--anhnt6

              AND    a.lhacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.lhtime BETWEEN g_min_time AND g_max_time
              AND    a.lhtime >= g_min_time
              AND    a.lhtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_lnhist;

    BEGIN
      IF (v_count > 0)
      THEN

        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
          AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        /*DELETE FROM bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction*/

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        RAISE;

    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) >= TRUNC(SYSDATE - 1)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN

      SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

                        DELETE FROM bk_account_history
            WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
            AND    TRUNC(post_time) = TRUNC(SYSDATE - 1)
                        AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) >= g_min_time
                        AND    TO_NUMBER(TO_CHAR(post_time - 1,
                                     'hh24') ||
                             TO_CHAR(post_time - 1,
                                     'mi') ||
                             TO_CHAR(post_time - 1,
                                     'mi')) < g_max_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            device_no, --anhnt6
                                                                            ln_time, --anhnt6
                                                                            os_balance, --anhnt6
                                                                            due_date) --anhnt6
                (SELECT                                        --a.lhtran,
                             --a.core_sn,
                             'LN'
                          || TO_CHAR (a.lhpstd)
                          || seq_core_sn_ln.NEXTVAL,
                          TO_DATE (a.lhefdt, 'yyyyddd'),
                          TO_DATE (a.lhpstd, 'yyyyddd'),
                          TRIM (a.lhdorc),
                          a.lhamt,
                          NVL (TRIM (a.lhcur), 'VND'),
                          a.lhcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.lhtext),
                          SUBSTR (a.lhtext, 11, LENGTH (a.lhtext)),
                          (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end),
                          SYSDATE,
                          '8',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.lhuser,
                          a.lhseqn,
                                        c.tran_sn,
                                        a.lhtran, --anhnt6
                                        a.lhtime, --anhnt6
                                        a.lhosbl, --anhnt6
                                        a.lhdudt -- anhnt6
                   FROM   sync_lnhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.lhtran) = d.tran_service_code(+)
                          AND TRIM (a.lhuser) = c.teller_id(+)
                          AND TRIM (a.lhseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.lhpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.lhuser)) = b.staff_name(+)
                          AND (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.lhtime BETWEEN g_min_time AND g_max_time
                                        AND a.lhtime >= g_min_time
                                        AND a.lhtime < g_max_time);


            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_lnhist_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_LNHIST_END_DAY');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_LNHIST_END_DAY');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_lnhist_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_lnhist_by_date_acct_sync
  (
    p_acct_no VARCHAR2,
    p_date    DATE
  ) IS
    v_count NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 2;

    SELECT TO_NUMBER(TO_CHAR(p_date,
                             'yyyy') || TO_CHAR(p_date,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.acct_no = p_acct_no);


    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_lnhist
              SELECT /*+ ALL_ROWS */
               a.lhtran,
               a.lhpstd,
               a.lhefdt,
               a.lhdorc,
               a.lhamt,
               a.lhcur,
               a.lhcamt,
               a.lhtext,
               a.lhacct,
               --NULL,    --'LN' || SEQ_CORE_SN_LN.NEXTVAL
               a.lhseqn,
               TRIM(a.lhuser),
               a.lhtime,
               a.lhosbl, --anhnt6
               a.lhdudt -- anhnt6
              FROM   STG.SI_HIS_LNHIST@STAGING_PRO a
              WHERE  a.lhdorc IN ('D',
                                  'C')
              AND    a.lhpstd = (v_checkpoint_date)
            --  AND    a.lhtran NOT IN (77,
            --                          129,
            --                          178, --179,
            --                          185)
            AND a.lhtran IN (912,993,990,914,922,915,121,101,962,974,988,42,41,976,35,15,30,62,61,23,22,21,145,143,497,496,781,889,926,102,811,812,906,43,964)--anhnt6
              AND    a.lhacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.lhtime BETWEEN g_min_time AND g_max_time
              AND    a.lhtime >= g_min_time
              AND    a.lhtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_lnhist;

    BEGIN
      IF (v_count > 0)
      THEN

        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no = p_acct_no; --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
          AND    TRUNC(post_time) = TRUNC(p_date)
          AND    rollout_acct_no = p_acct_no; --1:dd transaction

        DELETE FROM bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no = p_acct_no; --1:dd transaction

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        RAISE;

    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) = TRUNC(p_date)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN

      SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            device_no, --anhnt6
                                                                            ln_time, --anhnt6
                                                                            os_balance, --anhnt6
                                                                            due_date) --anhnt6
                (SELECT                                        --a.lhtran,
                             --a.core_sn,
                             'LN'
                          || TO_CHAR (a.lhpstd)
                          || seq_core_sn_ln.NEXTVAL,
                          TO_DATE (a.lhefdt, 'yyyyddd'),
                          TO_DATE (a.lhpstd, 'yyyyddd'),
                          TRIM (a.lhdorc),
                          a.lhamt,
                          NVL (TRIM (a.lhcur), 'VND'),
                          a.lhcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.lhtext),
                          SUBSTR (a.lhtext, 11, LENGTH (a.lhtext)),
                          (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end),
                          SYSDATE,
                          '8',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.lhuser,
                          a.lhseqn,
                                        c.tran_sn,
                                         a.lhtran, --anhnt6
                                        a.lhtime, --anhnt6
                                        a.lhosbl, --anhnt6
                                        a.lhdudt -- anhnt6
                   FROM   sync_lnhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.lhtran) = d.tran_service_code(+)
                          AND TRIM (a.lhuser) = c.teller_id(+)
                          AND TRIM (a.lhseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.lhpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.lhuser)) = b.staff_name(+)
                          AND (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.lhtime BETWEEN g_min_time AND g_max_time
                                        AND a.lhtime >= g_min_time
                                        AND a.lhtime < g_max_time);


            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_lnhist_by_date_acct_sync',
                                  NULL,
                                  'SUCC');

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_lnhist_by_date_acct_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;


  PROCEDURE proc_lnhist_by_date_sync(p_date DATE) IS
    v_count NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_count           := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 2;

    SELECT TO_NUMBER(TO_CHAR(p_date,
                             'yyyy') || TO_CHAR(p_date,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
       AND    bai.acct_type IN ('LN'));


    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_lnhist
              SELECT /*+ ALL_ROWS */
               a.lhtran,
               a.lhpstd,
               a.lhefdt,
               a.lhdorc,
               a.lhamt,
               a.lhcur,
               a.lhcamt,
               a.lhtext,
               a.lhacct,
               --NULL,    --'LN' || SEQ_CORE_SN_LN.NEXTVAL
               a.lhseqn,
               TRIM(a.lhuser),
               a.lhtime,
               a.lhosbl, --anhnt6
               a.lhdudt -- anhnt6
              FROM   STG.SI_HIS_LNHIST@STAGING_PRO a
              WHERE  a.lhdorc IN ('D',
                                  'C')
              AND    a.lhpstd = (v_checkpoint_date)
              --AND    a.lhtran NOT IN (77,
              --                        129,
              --                        178, --179,
              --                        185)
              AND a.lhtran IN (912,993,990,914,922,915,121,101,962,974,988,42,41,976,35,15,30,62,61,23,22,21,145,143,497,496,781,889,926,102,811,812,906,43,964)--anhnt6
              AND    a.lhacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.lhtime BETWEEN g_min_time AND g_max_time
              AND    a.lhtime >= g_min_time
              AND    a.lhtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    SELECT COUNT(1)
    INTO   v_count
    FROM   sync_lnhist;

    BEGIN
      IF (v_count > 0)
      THEN

        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no IN
               (SELECT acct_no
                 FROM   sync_account_info); --1:dd transaction

        INSERT INTO sync_bk_account_history
          SELECT *
          FROM   bk_account_history a
          WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
          AND    TRUNC(post_time) = TRUNC(p_date)
          AND    rollout_acct_no IN
                 (SELECT acct_no
                   FROM   sync_account_info); --1:dd transaction

        DELETE FROM bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(p_date)
        AND    rollout_acct_no IN
               (SELECT acct_no
                 FROM   sync_account_info); --1:dd transaction

      ELSIF (v_count = 0)
      THEN
        v_count := 10 / 0;

      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        RAISE;

    END;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) = TRUNC(p_date)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --SAVEPOINT s_insert_bulk;

    --noformat start
    LOOP
        BEGIN

      SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            device_no, --anhnt6
                                                                            ln_time, --anhnt6
                                                                            os_balance, --anhnt6
                                                                            due_date)
                (SELECT                                        --a.lhtran,
                             --a.core_sn,
                             'LN'
                          || TO_CHAR (a.lhpstd)
                          || seq_core_sn_ln.NEXTVAL,
                          TO_DATE (a.lhefdt, 'yyyyddd'),
                          TO_DATE (a.lhpstd, 'yyyyddd'),
                          TRIM (a.lhdorc),
                          a.lhamt,
                          NVL (TRIM (a.lhcur), 'VND'),
                          a.lhcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.lhtext),
                          SUBSTR (a.lhtext, 11, LENGTH (a.lhtext)),
                          (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end),
                          SYSDATE,
                          '8',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.lhuser,
                          a.lhseqn,
                                        c.tran_sn,
                                        a.lhtran ,--anhnt6
                                        a.lhtime, --anhnt6
                                        a.lhosbl, --anhnt6
                                        a.lhdudt -- anhnt6
                   FROM   sync_lnhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.lhtran) = d.tran_service_code(+)
                          AND TRIM (a.lhuser) = c.teller_id(+)
                          AND TRIM (a.lhseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.lhpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.lhuser)) = b.staff_name(+)
                          AND (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.lhtime BETWEEN g_min_time AND g_max_time
                                        AND a.lhtime >= g_min_time
                                        AND a.lhtime < g_max_time);


            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_lnhist_by_date_sync',
                                  NULL,
                                  'SUCC');

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_lnhist_by_date_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_lnhist_cif_sync IS
    v_min_date NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    p_min_value       := 0;
    v_checkpoint_time := NULL;
    g_loop_count      := 10;

    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_min_date        := 0;
    v_checkpoint_date := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 1;

    SELECT TO_NUMBER(TO_CHAR(SYSDATE - 1,
                             'yyyy') || TO_CHAR(SYSDATE - 1,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    SELECT TO_NUMBER(TO_CHAR(TRUNC(ADD_MONTHS(TRUNC(SYSDATE,
                                                    'MM'),
                                              -6)),
                             'yyyy') || TO_CHAR(TRUNC(ADD_MONTHS(TRUNC(SYSDATE,
                                                                       'MM'),
                                                                 -6)),
                                                'ddd'))
    INTO   v_min_date
    FROM   DUAL;

    INSERT INTO sync_cif_n
      SELECT cif_no,
             status
      FROM   sync_cif
      WHERE  status = 'N'
      AND    TYPE = 'LN';

    g_error_level := 2;

    INSERT INTO sync_account_info a
      (SELECT cif_no,
              acct_no
       FROM   bk_account_info
       WHERE  cif_no IN (SELECT cif_no
                         FROM   sync_cif_n sct)
       AND    acct_type IN ('LN'));


    g_error_level := 3;

    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_time        := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;

            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_lnhist
              SELECT /*+ ALL_ROWS */
               a.lhtran,
               a.lhpstd,
               a.lhefdt,
               a.lhdorc,
               a.lhamt,
               a.lhcur,
               a.lhcamt,
               a.lhtext,
               a.lhacct,
               --NULL,    --'LN' || SEQ_CORE_SN_LN.NEXTVAL
               a.lhseqn,
               TRIM(a.lhuser),
               a.lhtime,
               a.lhosbl, --anhnt6
               a.lhdudt -- anhnt6
              FROM   STG.SI_HIS_LNHIST@STAGING_PRO a
              WHERE  a.lhdorc IN ('D',
                                  'C')
              --AND    a.lhtran NOT IN (77,
              --                        129,
              --                        178, --179,
              --                        185)
              AND a.lhtran IN (912,993,990,914,922,915,121,101,962,974,988,42,41,976,35,15,30,62,61,23,22,21,145,143,497,496,781,889,926,102,811,812,906,43,964)--anhnt6
              AND    a.lhpstd >= v_min_date
              AND    a.lhpstd < (v_checkpoint_date)
              AND    a.lhacct IN (SELECT TO_NUMBER(acct_no)
                                  FROM   sync_account_info)
                    --AND    a.lhtime BETWEEN g_min_time AND g_max_time
              AND    a.lhtime >= g_min_time
              AND    a.lhtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_time        := g_min_time - g_limit_time;
              g_max_time        := g_max_time - g_limit_time;

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

          IF (i >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;

    g_error_level := 4;

    DELETE FROM bk_account_history
    WHERE  rollout_acct_no IN
           (SELECT acct_no
            FROM   sync_account_info sai)
    AND    TRUNC(post_time) < TRUNC(SYSDATE - 1)
    AND    (sync_type = 3 OR sync_type = 8) /*AND    sync_type NOT IN ('4','5')*/
    ;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) < TRUNC(SYSDATE - 1)
      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    --noformat start
    LOOP
        BEGIN
            SAVEPOINT s_insert_bulk;

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                                                            tran_sn,
                                                                            device_no,--anhnt6
                                                                            ln_time, --anhnt6
                                                                            os_balance, --anhnt6
                                                                            due_date) --anhnt6

                (SELECT                                        --a.lhtran,
                             --a.core_sn,
                             'LN'
                          || TO_CHAR (a.lhpstd)
                          || seq_core_sn_ln.NEXTVAL,
                          TO_DATE (a.lhefdt, 'yyyyddd'),
                          TO_DATE (a.lhpstd, 'yyyyddd'),
                          TRIM (a.lhdorc),
                          a.lhamt,
                          NVL (TRIM (a.lhcur), 'VND'),
                          a.lhcamt,
                          --'CNT',
                          --DECODE (b.staff_name, NULL, 'CNT', 'IBS'),
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          --TRIM(a.lhtext),
                          SUBSTR (a.lhtext, 11, LENGTH (a.lhtext)),
                          (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end),
                          SYSDATE,
                          '3',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.lhuser,
                          a.lhseqn,
                                        c.tran_sn,
                                        a.lhtran, --anhnt6
                                        a.lhtime, --anhnt6
                                        a.lhosbl, --anhnt6
                                        a.lhdudt -- anhnt6
                   FROM   sync_lnhist a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.lhtran) = d.tran_service_code(+)
                          AND TRIM (a.lhuser) = c.teller_id(+)
                          AND TRIM (a.lhseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.lhpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.lhuser)) = b.staff_name(+)
                          AND (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end) IN
                                     (SELECT   acct_no
                                        FROM   sync_account_info)
                          --AND a.lhtime BETWEEN g_min_time AND g_max_time
                                        AND a.lhtime >= g_min_time
                                        AND a.lhtime < g_max_time);
            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        EXCEPTION
            WHEN OTHERS
            THEN
                ROLLBACK TO s_insert_bulk;
                g_error_count := g_error_count + 1;
                g_min_time := g_min_time - g_limit_time;
                    g_max_time := g_max_time - g_limit_time;
                IF (g_error_count >= 10)
                THEN
                    RAISE;
                END IF;
        END;
    END LOOP;

    --noformat end

    /* DELETE FROM sync_cif
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    and    type = 'LN'; */

    UPDATE sync_cif
    SET    status = 'Y'
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    AND    "TYPE" = 'LN';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_lnhist_cif_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_LNHIST_NEW_CIF');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_LNHIST_NEW_CIF');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_lnhist_cif_sync',
                                    --'SYSTEM BUSY',
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  -- Date:11/05/2011
  -- Comments: Sync transfer schedule info
  -- ManhNV
  PROCEDURE proc_transfer_schedule_sync IS
    v_start_date DATE;
  BEGIN
    v_start_date := SYSDATE;

    INSERT INTO sync_transfer_schedule
      SELECT /*+ ALL_ROWS */
       TRIM(a.dracct), --Source account
       TRIM(a.cracct), --Receive account
       TRIM(a.orgdt7), --Start date
       TRIM(a.expdt7), --End date
       TRIM(a.trfamt), --Amount
       TRIM(a.freq), --  Frequency
       TRIM(a.freqcd), -- Frequency code
       TRIM(a.afent6), -- Date entered
       TRIM(a.afrsv1), --Active/Delete status
       TRIM(a.posseq), --posseq
       TRIM(a.ddmuid), --user id
       TRIM(a.ddmtim) --insert time
      FROM   svdatpv51.ddaftm@dblink_data a;

    --sync bk_tran_schedule
    MERGE INTO bk_tran_schedule eb_tran
    USING (SELECT --/*+ INDEX (gt_core, IX_GTT_TRANS_SCHEDULE_NO) */
            TRIM(gt_core.dracct) rollout_acct_no,
            TRIM(gt_core.cracct) bnfc_acct_no,
            TRIM(gt_core.trfamt) amount,
            TRIM(gt_core.afrsv1) status,
            TRIM(gt_core.posseq) posseq, --posseq
            TRIM(gt_core.ddmuid) ddmuid, --user id
            TRIM(gt_core.ddmtim) ddmtim, --insert time
            TRIM(gt_core.orgdt7) validated_date, --Start date
            TRIM(gt_core.expdt7) expired_date, --End date
            TRIM(gt_core.freq) frq_interval, --  Frequency
            TRIM(gt_core.freqcd) frq_type -- Frequency code
           FROM   sync_transfer_schedule gt_core
           WHERE  EXISTS (SELECT --/*+ INDEX(bai, BK_ACCOUNT_INFO_ACC_INDEX) */
                    bai.acct_no
                   FROM   bk_account_info bai
                   WHERE  bai.acct_no = gt_core.dracct)) src
    ON (eb_tran.posseq = src.posseq AND eb_tran.ddmuid = src.ddmuid AND eb_tran.insert_time = src.ddmtim)
    WHEN NOT MATCHED THEN
      INSERT
        (schdule_id,
         validated_date,
         expired_date,
         frq_interval,
         frq_type,
         end_type,
         is_lack_stop,
         frq_limit,
         posseq,
         ddmuid,
         insert_time)
      VALUES
        (seq_schdule_id.NEXTVAL,
         DECODE((LENGTH(TRIM(src.validated_date))),
                0,
                NULL,
                1,
                NULL,
                TO_DATE(TRIM(src.validated_date),
                        'YYYYDDD')),
         DECODE((LENGTH(TRIM(src.expired_date))),
                0,
                NULL,
                1,
                NULL,
                TO_DATE(TO_CHAR(TRIM(src.expired_date)),
                        'YYYYDDD')),
         TRIM(src.frq_interval),
         TRIM(src.frq_type),
         'E', --kieu ket thuc(E:expired date, C:count limit, L:lack stop)
         'Y', --ket thuc khi khong du so du
         0, --gioi han lan thanh toan
         src.posseq,
         src.ddmuid,
         src.ddmtim);

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_transfer_schedule_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_transfer_schedule_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
  END;

  PROCEDURE proc_all_info_cif_sync(v_type VARCHAR2) IS
    l_exists INTEGER;
  BEGIN
    SELECT COUNT(*)
    INTO   l_exists
    FROM   sync_cif a
    WHERE  a.status = 'N'
    AND    a.TYPE = v_type
    AND    ROWNUM = 1;

    IF l_exists = 1
    THEN
      IF (v_type = 'TM')
      THEN
        proc_tmtran_cif_sync();
      END IF;

      IF (v_type = 'DD')
      THEN
        proc_ddhist_cif_sync();
      END IF;

      IF (v_type = 'CD')
      THEN
        proc_cdhist_cif_sync();
      END IF;

      IF (v_type = 'LN')
      THEN
        proc_lnhist_cif_sync();
      END IF;

      IF (v_type = 'SL')
      THEN
        proc_salary_cif_sync();
      END IF;

      IF (v_type = 'TC')
      THEN
        proc_ddhist_trancode_cif_sync();
      END IF;
      /**
      * Step 1. Sync cif infomation
      * **/
      --ebank_cif_sync.proc_all_cif_sync();

      /**
      * Step 2. Sync account infomation
      * **/
      --ebank_account_sync.proc_account_cif_sync();

      /**
      * Step 3. Sync transaction infomation
      * **/
      --ebank_transaction_sync.proc_tran_hist_cif_sync();
    END IF;
  END;

  PROCEDURE proc_reset_sequence_sync IS
  BEGIN
    l_val        := 0;
    p_min_value  := 0;
    v_start_date := SYSDATE;

    --reset seq_core_sn_dd
    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_DD.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_DD increment by -' ||
                      (l_val - p_min_value) || ' minvalue ' || p_min_value;

    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_DD.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_DD increment by 1 minvalue ' ||
                      p_min_value;


    --reset seq_core_sn_fd
    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_FD.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_FD increment by -' ||
                      (l_val - p_min_value) || ' minvalue ' || p_min_value;

    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_FD.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_FD increment by 1 minvalue ' ||
                      p_min_value;


    --reset seq_core_sn_ln
    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_LN.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_LN increment by -' ||
                      (l_val - p_min_value) || ' minvalue ' || p_min_value;

    EXECUTE IMMEDIATE 'select SEQ_CORE_SN_LN.nextval from dual'
      INTO l_val;

    EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_LN increment by 1 minvalue ' ||
                      p_min_value;


    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_reset_sequence_sync',
                                  NULL,
                                  'SUCC');

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_RESET_SEQUENCE');
  EXCEPTION
    WHEN OTHERS THEN
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_reset_sequence_sync',
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_RESET_SEQUENCE');
  END;
END;

/
