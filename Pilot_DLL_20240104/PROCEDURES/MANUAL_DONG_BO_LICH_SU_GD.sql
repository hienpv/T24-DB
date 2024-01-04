--------------------------------------------------------
--  DDL for Procedure MANUAL_DONG_BO_LICH_SU_GD
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "IBS"."MANUAL_DONG_BO_LICH_SU_GD" 
(
  ACCOUNT_NO IN VARCHAR2 
, FROMDATE IN DATE 
, TODATE IN DATE 
) AS 
BEGIN

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
              FROM HSTAGING.SI_HIS_DDHIST@HSTAGING_PRO_CORE a

             WHERE a.trdate >= to_char(FROMDATE,'yyyyDDD')
             and a.trdate <= to_char(TODATE,'yyyyDDD')
               AND a.dorc IN ('D', 'C')
               AND a.trancd NOT IN (77,
                                    129,
                                    178, --179,
                                    185)
               AND a.tracct = ACCOUNT_NO;



    delete bk_account_history where rollout_acct_no = ACCOUNT_NO and tran_time >= trunc(FROMDATE)  and tran_time < trunc(TODATE) + 1;



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
                                          trace_code) --anhntt6)
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
               WHERE (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) = ACCOUNT_NO);


               delete  sync_ddhist;
           commit;

END MANUAL_DONG_BO_LICH_SU_GD;

/
