--------------------------------------------------------
--  DDL for Procedure PROC_DDHIST_BY_TODATE_CIF_SYNC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "IBS"."PROC_DDHIST_BY_TODATE_CIF_SYNC" (p_date  DATE,
                                                         pCif_no varchar2) IS
  v_count           NUMBER;
  v_start_date      DATE;
  p_min_value       NUMBER;
  l_val             NUMBER;
  v_checkpoint_time DATE;
  v_is_sync         CHAR(1);

  g_error_level NUMBER;

  g_loop_count      NUMBER;
  g_limit_time      NUMBER;
  g_min_time        NUMBER;
  g_max_time        NUMBER;
  g_time_count      NUMBER;
  g_error_sub_count NUMBER;
  g_error_count     NUMBER;
  v_checkpoint_date NUMBER;
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

  SELECT TO_NUMBER(TO_CHAR(p_date, 'yyyy') || TO_CHAR(p_date, 'ddd'))
    INTO v_checkpoint_date
    FROM DUAL;

  INSERT INTO sync_account_info a
    (SELECT bai.cif_no, bai.acct_no
       FROM bk_account_info bai
      WHERE bai.cif_no IN (
                           SELECT cif_no FROM bb_corp_info)
        AND bai.acct_type IN ('CA', 'SA')
        AND bai.cif_no = pCif_no);

  --Get all data from DDHIST core
  g_error_level := 1;

  FOR i IN 1 .. g_loop_count LOOP
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
              FROM svhispv51.ddhist@dblink_data a

             WHERE a.trdate <= (v_checkpoint_date)
               AND a.dorc IN ('D', 'C')
               AND a.trancd NOT IN (77,
                                    129,
                                    178, --179,
                                    185)
               AND a.tracct = 02001010018667
                  --AND    a.trtime BETWEEN g_min_time AND g_max_time
               AND a.trtime >= g_min_time
               AND a.trtime < g_max_time;

          COMMIT;
          --khong co them ban ghi nao
          EXIT WHEN(g_max_time > g_time_count);
        EXCEPTION
          WHEN OTHERS THEN
            ROLLBACK TO s_insert_temp_l;
            g_error_sub_count := g_error_sub_count + 1;
            g_min_time        := g_min_time - g_limit_time;
            g_max_time        := g_max_time - g_limit_time;

            IF (g_error_sub_count >= 10) THEN
              RAISE;
            END IF;
            --DBMS_LOCK.SLEEP(10);
        END;
      END LOOP;

      EXIT;
    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK TO s_insert_temp;

        IF (i >= 10) THEN
          RAISE;
        END IF;
        --DBMS_LOCK.SLEEP(10);
    END;
  END LOOP;

  g_error_level := 2;

  SELECT COUNT(1) INTO v_count FROM sync_ddhist;

  --Check dam bao buoc 1 chay thanh cong, va co du lieu trong sync_ddhist
  --Dam bao cho truong hop job chay truoc khi chay batch
  BEGIN

    IF (v_count > 0) THEN
      DELETE FROM sync_bk_account_history
       WHERE (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
         AND TRUNC(post_time) <= TRUNC(p_date)
         AND rollout_acct_no IN (SELECT acct_no FROM sync_account_info); --1:dd transaction

      INSERT INTO sync_bk_account_history
        SELECT *
          FROM bk_account_history a
         WHERE (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
           AND TRUNC(post_time) <= TRUNC(p_date)
           AND rollout_acct_no IN (SELECT acct_no FROM sync_account_info);

      DELETE FROM bk_account_history
       WHERE (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
         AND TRUNC(post_time) <= TRUNC(p_date)
         AND rollout_acct_no IN (SELECT acct_no FROM sync_account_info); --1:dd transaction

    ELSIF (v_count = 0) THEN
      v_count := 10 / 0;

    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      RAISE;
  END;

  g_error_level := 10;

  INSERT INTO sync_tranmap
    SELECT /*+ ALL_ROWS */
     a.tran_sn, a.teller_id, a.host_tran_sn, a.host_real_date, a.sender_id
      FROM bec.bec_msglog@dblink_tranmap a
    --FROM   bec.bec_msglog2 a
     WHERE a.sorn = 'Y'
       AND TRUNC(TO_DATE(a.message_date, 'yyyymmddhh24mi')) <= TRUNC(p_date)
       AND LENGTH(a.tran_sn) < 21;

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
                        LPAD (a.tracct, 14, '0'),
                        SYSDATE,
                        '6',                            --dd transaction
                        'SUCC',
                        DECODE (d.tran_service_code,
                                NULL, 'RTR001',
                                'RTR002'),
                        a.truser,
                        a.seq,
                    c.tran_sn
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
                        AND LPAD (a.tracct, 14, '0') IN
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
                                  SUBSTR(TO_CHAR(g_error_level) || 'Error ' ||
                                         TO_CHAR(SQLCODE) || ': ' ||
                                         SQLERRM,
                                         1,
                                         1000),
                                  'FAIL');
    /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
    SQLERRM,
    1,
    255));*/
END;

/
