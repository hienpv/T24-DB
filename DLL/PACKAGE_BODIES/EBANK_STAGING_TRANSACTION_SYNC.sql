--------------------------------------------------------
--  DDL for Package Body EBANK_STAGING_TRANSACTION_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "EBANK_STAGING_TRANSACTION_SYNC" is

  PROCEDURE proc_run_sync_vipCop IS
    synDate   date;
    isRunHist integer;
  begin
    synDate := sysdate - 1;
    checkSyncDDHist(synDate);
  end;

  /**
  Check xem du lieu dong bo da day chua?
  Dua vaof 1 so yeu to khi thuc hien dong bo ddhist thi se xoa
  tat ca cac giao dich dong bo trong ngay truoc do co
  sync_type = '6';
  substr(bah.core_sn, 0, 2) <> 'DD'

  */

  procedure checkSyncDDHist(synDate Date) IS
    iCount            number(2);
    iCheckHis         number(10);
    iCheckHost        number(10);
    v_checkpoint_date varchar2(10);
    counter           number(2);

  BEGIN
    SELECT TO_NUMBER(TO_CHAR(synDate, 'yyyy') || TO_CHAR(synDate, 'ddd'))
      INTO v_checkpoint_date
      FROM DUAL;
    -- Ebank_Transaction_Sync.proc_ddhist_by_date_sync(synDate);
    select count(1)
      into iCount
      from bk_account_history bah
     where bah.sync_type = '6'
       and substr(bah.core_sn, 0, 2) <> 'DD'
       and trunc(bah.tran_time) = trunc(synDate);
    if (iCount > 1) then
      Begin

        delete from sync_ddhist;
        commit;
        Ebank_Transaction_Sync.proc_ddhist_by_date_sync(synDate);
      End;
    end if;

    proc_ddhist_sync_vipCop(synDate);
    proc_cdhist_bb_vip_sync(synDate);
    proc_lnhist_bb_vip_sync(synDate);
    --

    /*
    Kiem tra neu count cua DB IB va host neu bang nhau
    thi thoi khong chay tiep toi da 3 lan
    */

  /*  INSERT INTO sync_account_info a
      (SELECT bai.cif_no, bai.acct_no
         FROM bk_account_info bai
        WHERE bai.cif_no IN
              (SELECT cif_no
                 FROM BB_ACCOUNT_VIP_SYNC
                where cif_no in (SELECT cif_no FROM bb_corp_info))
          AND bai.acct_type IN ('CA', 'SA'));

    FOR counter IN 1 .. 4 LOOP

      select count(1)
        into iCheckHis
        from bk_account_history bah1
       where to_char(bah1.tran_time, 'yyyymmdd') =
             to_char(synDate, 'yyyymmdd')
         and bah1.sync_type = '6'
         and bah1.rollout_acct_no IN
             (SELECT acct_no FROM sync_account_info);
      -- lay so luong tu trong core

      SELECT count(1)
        into iCheckHost
        FROM STG.SI_HIS_DDHIST@STAGING_PRO a
       WHERE a.trdate = (v_checkpoint_date)
         AND a.dorc IN ('D', 'C')
         AND a.trancd NOT IN (77,
                              129,
                              178, --179,
                              185)
         AND a.tracct IN (SELECT to_number(acct_no) FROM sync_account_info);

      if (iCheckHis < iCheckHost) then
        proc_ddhist_sync_vipCop(synDate);
      else
        return;
      end if;

    END LOOP;*/

    -- het kiem tra
    commit;
    return;
  END checkSyncDDHist;

  /*
  Dong bo rieng cho 1 so khach hang vip
  */

  PROCEDURE proc_ddhist_sync_vipCop(p_date DATE) IS
    v_count NUMBER;

    v_start_date  DATE;
    p_min_value   NUMBER;
    v_errorr      varchar2(3000) := 'Error: ';
    v_is_sync     CHAR(1);
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

    -- Xoa cac bang tam
    delete from sync_account_info;
    delete from SYNC_DDHIST_COP_VI;
    delete from sync_ib_bk_account_checkkey;
    delete from sync_tranmap;
    commit;
    --Het Xoa cac bang tam
    SELECT TO_NUMBER(TO_CHAR(p_date, 'yyyy') || TO_CHAR(p_date, 'ddd'))
      INTO v_checkpoint_date
      FROM DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no, bai.acct_no
         FROM bk_account_info bai
        WHERE bai.cif_no IN
              (SELECT cif_no
                 FROM BB_ACCOUNT_VIP_SYNC
                where cif_no in (SELECT cif_no FROM bb_corp_info))
          AND bai.acct_type IN ('CA', 'SA'));
    --  lay du lieu cua bang bk_account_history vao bang  sync_ib_bk_account_checkkey

    insert into sync_ib_bk_account_checkkey
      (teller_id,
       tm_seq,
       amount,
       rollout_acct_no,
       trace_code,
       dc_sign,
       tran_time,
       tran_date,
       checkkey)
      (select teller_id,
              tm_seq,
              amount,
              Ltrim(rollout_acct_no, '0'),
              trace_code,
              dc_sign,
              to_char(tran_time, 'yyyydddhh24miss'),
              to_char(bah1.tran_time, 'yyyymmdd'),

              trim(teller_id) || trim(tm_seq) || trim(amount) ||
              trim(Ltrim(trim(rollout_acct_no), '0')) /* || trim(trace_code)*/
              || trim(dc_sign) ||
              trim(to_char(tran_time, 'yyyydddhh24miss'))

         from bk_account_history bah1
        where to_char(bah1.tran_time, 'yyyymmdd') =
              to_char(p_date, 'yyyymmdd')
          and bah1.rollout_acct_no IN
              (SELECT acct_no FROM sync_account_info));
    commit;
    -- Het lay du lieu bk_account_history
    --Get all data from DDHIST core
    g_error_level := 1;

    -- FOR i IN 1 .. g_loop_count LOOP
    -- try g_loop_count times

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

            INSERT INTO SYNC_DDHIST_COP_VI
              (trancd,
               trdat6,
               dorc,
               amt,
               trctyp,
               camt,
               trefth,
               tracct,
               trdate,
               treffd,
               trtime,
               seq,
               truser,
               auxtrc,
               checkkey)
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
               TRIM(a.auxtrc),
               trim(a.truser) || trim(a.seq) || a.amt ||
               Ltrim(trim(a.tracct), '0') || /*trim(a.auxtrc) ||*/
               trim(a.dorc) ||
               to_number(a.trdate || LPAD(a.trtime, 6, '0'))
                from STAGING.SI_HIS_DDDHIST@STAGING_PRO_CORE a
              -- FROM sthistrn.ddhist@dblink_data a
              --  select * from STAGING.SI_HIS_DDDHIST@DBLINK_DATA_STAGING
               WHERE a.trdate = (v_checkpoint_date)
                 AND a.dorc IN ('D', 'C')
                 AND a.trancd NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
                 AND a.tracct IN
                     (SELECT to_number(acct_no) FROM sync_account_info)
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

    SELECT COUNT(1) INTO v_count FROM SYNC_DDHIST_COP_VI;

    --Check dam bao buoc 1 chay thanh cong, va co du lieu trong sync_ddhist
    --Dam bao cho truong hop job chay truoc khi chay batch
    g_error_level := 10;
    IF (v_count > 0) then
      Begin

        INSERT INTO sync_tranmap
          SELECT /*+ ALL_ROWS */
           a.tran_sn,
           a.teller_id,
           a.host_tran_sn,
           a.host_real_date,
           a.sender_id
            FROM bec.bec_msglog@dblink_tranmap a
          --FROM   bec.bec_msglog2 a
           WHERE a.sorn = 'Y'
             AND TRUNC(TO_DATE(a.message_date, 'yyyymmddhh24mi')) =
                 TRUNC(p_date)
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
                                                    tran_sn,
                                                    trace_code)
                        (SELECT
                                     'DD'
                                  || TO_CHAR (a.trdate)
                                  || seq_core_sn_dd.NEXTVAL,

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

                                  DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
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
                                                a.auxtrc
                           FROM   SYNC_DDHIST_COP_VI a, sync_tran_code d, sync_tranmap c
                          WHERE   TRIM (a.auxtrc) = d.tran_service_code(+)
                                  AND TRIM (a.truser) = c.teller_id(+)
                                  AND TRIM (a.seq) = c.host_tran_sn(+)
                                  AND TRUNC (TO_DATE (a.trdate, 'yyyyddd')) =
                                         TRUNC(TO_DATE (c.host_real_date(+),
                                                        'yyyyddd'))
                                  AND a.checkkey not in  (SELECT checkkey FROM sync_ib_bk_account_checkkey)
                                  AND 
                                  (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) IN
                                             (SELECT   (case when length(acct_no) = 13 then LPAD (acct_no, 14, '0') else TO_CHAR(acct_no) end)
                                                FROM   sync_account_info)

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

        EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA_STAGING';

        ebank_sync_util.proc_sync_log(v_start_date,
                                      SYSDATE,
                                      'proc_ddhist_coperate_by_date_sync_vip',
                                      NULL,
                                      'SUCC');
      end;
    end if;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      v_errorr := v_errorr || sqlerrm;

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_ddhist_by_date_sync_cif_vip',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');

  END proc_ddhist_sync_vipCop;

  PROCEDURE proc_cdhist_bb_vip_sync(p_date DATE) IS
    v_count           NUMBER;
    v_start_date      DATE;
    p_min_value       NUMBER;
    l_val             NUMBER;
    v_checkpoint_time DATE;
    v_is_sync         CHAR(1);
    g_error_level     NUMBER;

    g_loop_count      NUMBER;
    g_limit_time      NUMBER;
    g_min_time        NUMBER;
    g_max_time        NUMBER;
    g_time_count      NUMBER;
    g_error_sub_count NUMBER;
    g_error_count     NUMBER;
    v_checkpoint_date NUMBER;

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

    SELECT TO_NUMBER(TO_CHAR(p_date, 'yyyy') || TO_CHAR(p_date, 'ddd'))
      INTO v_checkpoint_date
      FROM DUAL;

    -- Xoa cac bang du lieu tam truoc khi dong bo tránh trung du lieu

    delete from sync_account_info;
    delete from SYNC_CDHIST_BBVIP;
    delete from sync_bk_bbVip_cd;
    commit;
    -- Het xoa du li?u

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no, bai.acct_no
         FROM bk_account_info bai
        WHERE bai.cif_no IN (SELECT cif_no FROM BB_ACCOUNT_VIP_SYNC)
          AND bai.acct_type IN ('FD')
          AND bai.p_acct_no IS NOT NULL);

    g_error_level := 3;

    g_min_time   := 0;
    g_max_time   := 0;
    g_limit_time := 60000;
    g_time_count := 240000;

    -- Day du lieu trong bang bk_account_history vào 1 bang tam lam dieu kien so sanh
    INSERT INTO sync_bk_bbVip_cd
      (checkkey,
       trace_code,
       dc_sign,
       currency_code,
       rollout_acct_no,
       amount,
       post_time,
       tran_time,
       remark,
       teller_id,
       tm_seq)
      select LTRIM(rollout_acct_no, '0') || trim(bah.dc_sign) ||
             trim(bah.currency_code) || to_char(amount) ||
             trim(bah.teller_id) || trim(bah.tm_seq) ||
             to_char(bah.post_time, 'yyyyddd'),

             bah.trace_code,
             bah.dc_sign,
             bah.currency_code,
             rollout_acct_no,
             amount,
             post_time,
             tran_time,
             remark,
             teller_id,
             tm_seq
        from bk_account_history bah
       where trunc(tran_time) = trunc(p_date)
         and bah.rollout_acct_no in (select acct_no from sync_account_info);

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

            INSERT INTO SYNC_CDHIST_BBVIP
              (chtran,
               CHDORC,
               CHAMT,
               CHCURR,
               CHCAMT,
               CHVARF,
               CHACCT,
               CHEFDT,
               CHPSTD,
               CHSEQN,
               CHUSER,
               CHTIME,
               CHECKKEY)
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
               a.chtime,
               -- checkkey
               chacct || trim(chdorc) || trim(chcurr) || to_char(chamt) ||
               trim(TRIM(a.chuser)) || trim(a.chseqn) || a.chpstd
                FROM STAGING.SI_HIS_CDDHIST@STAGING_PRO_CORE a
              -- FROM sthistrn.cdhist@dblink_data a
               WHERE a.chdorc IN ('D', 'C')
                 AND a.chafft in ('B', 'C') --anhnt6
                 AND a.chpstd = (v_checkpoint_date)
                 AND a.chtran NOT IN (77,
                                      129,
                                      178, --179,
                                      185)
                 AND a.chacct IN
                     (SELECT TO_NUMBER(acct_no) FROM sync_account_info)
                    --AND    a.chtime BETWEEN g_min_time AND g_max_time
                 AND a.chtime >= g_min_time
                 AND a.chtime < g_max_time;

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

    g_error_level := 4;

    SELECT COUNT(1) INTO v_count FROM SYNC_CDHIST_BBVIP;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
        FROM bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
       WHERE a.sorn = 'Y'
         AND TRUNC(TO_DATE(a.message_date, 'yyyymmddhh24mi')) =
             TRUNC(p_date)
         AND LENGTH(a.tran_sn) < 21;

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
                   FROM   SYNC_CDHIST_BBVIP a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.chtran) = d.tran_service_code(+)
                          AND TRIM (a.chuser) = c.teller_id(+)
                          AND TRIM (a.chseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.chpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          AND a.checkkey not in (select checkkey from sync_bk_bbVip_cd)
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

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA_STAGING';

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
  END proc_cdhist_bb_vip_sync;

  PROCEDURE proc_lnhist_bb_vip_sync(p_date DATE) IS
    v_count           NUMBER;
    v_start_date      DATE;
    p_min_value       NUMBER;
    l_val             NUMBER;
    v_checkpoint_time DATE;
    v_is_sync         CHAR(1);
    g_error_level     NUMBER;

    g_loop_count      NUMBER;
    g_limit_time      NUMBER;
    g_min_time        NUMBER;
    g_max_time        NUMBER;
    g_time_count      NUMBER;
    g_error_sub_count NUMBER;
    g_error_count     NUMBER;
    v_checkpoint_date NUMBER;

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

    delete from sync_bk_bbVip_ln;
    delete from SYNC_LNHIST_BBVIP;
    commit;
    SELECT TO_NUMBER(TO_CHAR(p_date, 'yyyy') || TO_CHAR(p_date, 'ddd'))
      INTO v_checkpoint_date
      FROM DUAL;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no, bai.acct_no
         FROM bk_account_info bai
        WHERE bai.cif_no IN (SELECT cif_no FROM BB_ACCOUNT_VIP_SYNC)
          AND bai.acct_type IN ('LN'));

    g_error_level := 3;

    --quanld Insert du lieu tu bk_account_history vao bang tam

    INSERT INTO sync_bk_bbVip_ln
      (checkkey,
       trace_code,
       dc_sign,
       currency_code,
       rollout_acct_no,
       amount,
       post_time,
       tran_time,
       remark,
       teller_id,
       tm_seq)
      select LTRIM(rollout_acct_no, '0') || trim(bah.dc_sign) ||
             trim(bah.currency_code) || to_char(amount) ||
             trim(bah.teller_id) || trim(bah.tm_seq) ||
             to_char(bah.post_time, 'yyyyddd'),
             bah.trace_code,
             bah.dc_sign,
             bah.currency_code,
             rollout_acct_no,
             amount,
             post_time,
             tran_time,
             remark,
             teller_id,
             tm_seq
        from bk_account_history bah
       where trunc(tran_time) = trunc(p_date)
         and bah.rollout_acct_no in (select acct_no from sync_account_info);

    --Het
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

            INSERT INTO SYNC_LNHIST_BBVIP
              (lhtran,
               lhpstd,
               lhefdt,
               lhdorc,
               lhamt,
               lhcur,
               lhcamt,
               lhtext,
               lhacct,
               --NULL,    --'LN' || SEQ_CORE_SN_LN.NEXTVAL
               lhseqn,
               lhuser,
               lhtime,
               lhosbl, --anhnt6
               lhdudt, -- anhnt6
               checkkey)
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
               a.lhdudt, -- anhnt6
               /*
                  LTRIM(rollout_acct_no, '0') || trim(bah.dc_sign) ||
               trim(bah.currency_code) || to_char(amount) ||
               trim(bah.teller_id) || trim(bah.tm_seq) ||
               to_char(bah.post_time, 'yyyyddd')
                 */
               --Tai khoan || loai giao dich || loai tien || so tien ||
               -- teller || so seq ||  ngay giao dich|| gio giao dich
               a.lhacct || trim(lhdorc) || trim(lhcur) || trim(lhamt) ||
               trim(a.lhuser) || trim(a.lhseqn) || a.lhpstd
                FROM STAGING.SI_DAT_LNDHIS@STAGING_PRO_CORE a
              -- FROM sthistrn.lnhist@dblink_data a
               WHERE a.lhdorc IN ('D', 'C')
                 AND a.lhpstd = (v_checkpoint_date)
                    --              AND    a.lhtran NOT IN (77, --anhnt
                    --                                      129,--anhnt
                    --                                      178, --179, --anhnt
                    --                                      185)--anhnt
                 AND a.lhtran IN ( --anhnt6
                                  912,
                                  993,
                                  990,
                                  914,
                                  922,
                                  915,
                                  121,
                                  101,
                                  962,
                                  974,
                                  988,
                                  42,
                                  41,
                                  976,
                                  35,
                                  15,
                                  30,
                                  62,
                                  61,
                                  23,
                                  22,
                                  21,
                                  145,
                                  143,
                                  497,
                                  496,
                                  781,
                                  889,
                                  926,
                                  102,
                                  811,
                                  812,
                                  906,
                                  43,
                                  964,
                                  148

                                  ) --anhnt6
                 AND a.lhacct IN
                     (SELECT TO_NUMBER(acct_no) FROM sync_account_info)
                    --AND    a.lhtime BETWEEN g_min_time AND g_max_time
                 AND a.lhtime >= g_min_time
                 AND a.lhtime < g_max_time;

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

    g_error_level := 4;

    SELECT COUNT(1) INTO v_count FROM SYNC_LNHIST_BBVIP;

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT /*+ ALL_ROWS */
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
        FROM bec.bec_msglog@dblink_tranmap a
      --FROM   bec.bec_msglog2 a
       WHERE a.sorn = 'Y'
         AND TRUNC(TO_DATE(a.message_date, 'yyyymmddhh24mi')) =
             TRUNC(p_date)
         AND LENGTH(a.tran_sn) < 21;

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
                                        a.lhtran ,--anhnt6
                                        a.lhtime, --anhnt6
                                        a.lhosbl, --anhnt6
                                        a.lhdudt -- anhnt6
                   FROM   SYNC_LNHIST_BBVIP a, sync_tran_code d, sync_tranmap c --sync_teller b
                  WHERE       TRIM (a.lhtran) = d.tran_service_code(+)
                          AND TRIM (a.lhuser) = c.teller_id(+)
                          AND TRIM (a.lhseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.lhpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          --AND TRIM (TRIM (a.lhuser)) = b.staff_name(+)

                          AND a.checkkey not in (select checkkey from sync_bk_bbVip_ln)
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

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA_STAGING';

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

  PROCEDURE proc_run_test_vipCop IS
    synDate   date;
    isRunHist integer;
    icount    integer;
    endDate   date;
  begin
    synDate := to_date('20130301', 'yyyymmdd');
    endDate := to_date('20130410', 'yyyymmdd');
    /* select sysdate-278 into synDate  from dual; */

    /* checkSyncDDHist(synDate);*/

    loop
      begin
        synDate := synDate + 1;
        proc_ddhist_sync_vipCop(synDate);
        proc_cdhist_bb_vip_sync(synDate);
        proc_lnhist_bb_vip_sync(synDate);
        EXIT WHEN(synDate = endDate);
      end;
    end loop;
    /*  select count(1)
       into isRunHist
       from user_scheduler_jobs job
      where job.JOB_ACTION = 'ebank_transaction_sync.proc_cdhist_sync'
        and state = 'SCHEDULED';

    if (isRunHist > 0) then
       select count(1)
         into isRunHist
         from sync_log
        where sync_case = 'proc_ddhist_sync'
          and TRUNC(end_date) = TRUNC(synDate);
     end if;*/
    /*   if (isRunHist > 0) then

    end if;*/
  end;

end EBANK_STAGING_TRANSACTION_SYNC;

/
