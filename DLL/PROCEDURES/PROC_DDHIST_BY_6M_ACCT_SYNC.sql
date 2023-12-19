--------------------------------------------------------
--  DDL for Procedure PROC_DDHIST_BY_6M_ACCT_SYNC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PROC_DDHIST_BY_6M_ACCT_SYNC" (p_acct_no VARCHAR2) is
  v_count      NUMBER;
  p_date       date;
  v_start_date DATE;

  g_error_level NUMBER;

  v_checkpoint_date NUMBER;
BEGIN
  v_start_date := SYSDATE;

  v_checkpoint_date := 0;
  v_count           := 0;

  g_error_level := 3;
  p_date        := sysdate - 180;

  SELECT TO_NUMBER(TO_CHAR(p_date, 'yyyy') || TO_CHAR(p_date, 'ddd'))
    INTO v_checkpoint_date
    FROM DUAL;


  INSERT INTO sync_account_info a
    (SELECT bai.cif_no, bai.acct_no
       FROM bk_account_info bai
      WHERE bai.acct_no = p_acct_no);

  --Get all data from DDHIST core
  g_error_level := 1;

  -- try g_loop_count times
  BEGIN

    SAVEPOINT s_insert_temp;

    BEGIN
      SAVEPOINT s_intert_temp_l;
        delete from sync_ddhist;
        commit;
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
          FROM STAGING.SI_HIS_DDHIST@STAGING_PRO_CORE a
         WHERE a.trdate > v_checkpoint_date
           AND a.dorc IN ('D', 'C')
           AND a.trancd NOT IN (77,
                                129,
                                178, --179,
                                185)
           AND a.tracct = to_number(p_acct_no);

      COMMIT;
      --khong co them ban ghi nao

    EXCEPTION
      WHEN OTHERS THEN
        ROLLBACK TO s_insert_temp_l;

    END;

  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK TO s_insert_temp;

  END;

  g_error_level := 2;

  SELECT COUNT(1) INTO v_count FROM sync_ddhist;

  --Check dam bao buoc 1 chay thanh cong, va co du lieu trong sync_ddhist
  --Dam bao cho truong hop job chay truoc khi chay batch

  --SAVEPOINT s_insert_bulk;

  --noformat start

      BEGIN
          SAVEPOINT s_insert_bulk;



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
                                          trace_code --anhntt6
                                                                          )
              (SELECT /*+ INDEX(sync_ddhist, IDX_SYNC_DDHIST) */
               'DD' || TO_CHAR(a.trdate) || seq_core_sn_dd.NEXTVAL,
               TO_DATE((a.trdate || ':' || LPAD(a.trtime, 6, '0')), 'yyyyddd:hh24miss'),
               TO_DATE((a.treffd || ':' || LPAD(a.trtime, 6, '0')), 'yyyyddd:hh24miss'),
               TRIM(a.dorc),
               a.amt,
               TRIM(a.trctyp),
               a.camt,
               'CNT',
               SUBSTR(a.trefth, 11, LENGTH(a.trefth)),
               (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
               SYSDATE,
               '6', --dd transaction
               'SUCC',
               'RTR001',
               a.truser,
               a.seq,
               '',
              trim( a.auxtrc)
                FROM sync_ddhist a --sync_teller b
               WHERE (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end)= p_acct_no);
                          COMMIT;

                      EXCEPTION
                          WHEN OTHERS
                          THEN
                              ROLLBACK TO s_insert_bulk;

                      END;

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
                                  SUBSTR(TO_CHAR(g_error_level) || 'Error ' ||
                                         TO_CHAR(SQLCODE) || ': ' ||
                                         SQLERRM,
                                         1,
                                         1000),
                                  'FAIL');
end proc_ddhist_by_6M_acct_sync;

/
