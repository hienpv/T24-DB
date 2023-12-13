--------------------------------------------------------
--  DDL for Package Body EBANK_DWH_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "EBANK_DWH_SYNC" 
 IS

  PROCEDURE proc_ddft_transaction  IS -- chay 1 lan trong ngay, lich su truoc hien tai 1 ngay.
  BEGIN
    MERGE INTO   bk_account_history c
                        USING   (
                        SELECT distinct a.trn_date,a.channel_trn,a.seq_number,a.user_id,a.trf_bank_sk,a.related_person,
                        (case when length(b.account_no) = 13 then LPAD (b.account_no, 14, '0') else TO_CHAR(b.account_no) end) account_no,
                        a.related_account
                                   FROM   dwh.ddft_transaction@dblink_balance a inner join dwh.dddm_account@dblink_balance b
                                on a.account_no_sk = b.account_no_sk
                                where a.channel_trn is not null and trn_date > to_number(to_char(sysdate-3,'yyyyddd')) -- 18072013
                                and a.dorc_ind is not null and (related_person is not null or related_account is not null or trf_bank_sk is not null)
                                and a.trn_sk != 70351 ) src
                           ON   (c.trace_code is not null and c.rollout_acct_no = src.account_no and  to_char(c.tran_time,'yyyyddd') = src.trn_date and c.trace_code = src.channel_trn and c.tm_seq = src.seq_number
                           and trim(c.teller_id) = src.user_id)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.beneficiary_acct_no = trim(src.related_account),
                           c.beneficiary_acct_bank = trim(src.trf_bank_sk),
                           c.beneficiary_acct_name = trim(src.related_person);

                       commit;
      MERGE INTO   bk_account_history c
      USING   (
      SELECT distinct a.trn_date,a.channel_trn,a.seq_number,a.user_id,a.trf_bank_sk,a.related_person,
      (case when length(b.account_no) = 13 then LPAD (b.account_no, 14, '0') else TO_CHAR(b.account_no) end)  account_no,
      a.related_account
                 FROM   dwh.ddft_transaction@dblink_balance a inner join dwh.dddm_account@dblink_balance b
              on a.account_no_sk = b.account_no_sk
              where a.channel_trn is not null and trn_date > to_number(to_char(sysdate-3,'yyyyddd'))
              and a.dorc_ind is not null and (related_person is not null or related_account is not null or trf_bank_sk is not null)
              and a.trn_sk = 70351 ) src
         ON   (c.trace_code is not null and c.rollout_acct_no = src.account_no and  to_char(c.tran_time,'yyyyddd') = src.trn_date and c.trace_code = src.channel_trn and c.tm_seq = src.seq_number
         and trim(c.teller_id) = src.user_id)
 WHEN MATCHED
 THEN
     UPDATE SET
         c.beneficiary_acct_no = trim(src.related_account),
         c.beneficiary_acct_bank = trim(src.trf_bank_sk),
         c.beneficiary_acct_name = trim(src.related_person);
         commit;

  END;
   PROCEDURE proc_crtb_tmtran_ol IS -- lich su trong ngay chay 3 tieng trong ngay
  BEGIN
    MERGE INTO   bk_account_history c
                        USING   (
                        SELECT distinct a.trn_date,a.channel_trn,a.trn_seq,a.maker_id,a.trf_bank,a.related_person,
                        (case when length(a.account_no) = 13 then LPAD (a.account_no, 14, '0') else TO_CHAR(a.account_no) end) account_no,
                        a.related_account
                                   FROM   ods.crtb_tmtran_ol@dblink_balance a
                                where a.channel_trn is not null and trn_date > to_number(to_char(sysdate-1,'yyyyddd')) and host_affect_code = 'B'
                                and a.dorc is not null and (related_person is not null or related_account is not null or trf_bank is not null)
                                and a.host_trn_code != 70351 ) src
                           ON   (c.trace_code is not null and c.rollout_acct_no = src.account_no and  to_char(c.tran_time,'yyyyddd') = src.trn_date and c.trace_code = src.channel_trn and c.tm_seq = src.trn_seq
                           and trim(c.teller_id) = src.maker_id)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.beneficiary_acct_no = trim(src.related_account),
                           c.beneficiary_acct_bank = trim(src.trf_bank),
                           c.beneficiary_acct_name = trim(src.related_person);
                           commit;
  END;







   PROCEDURE proc_ddft_transaction_by_date  (p_date DATE) IS -- chay 1 lan trong ngay, lich su truoc hien tai 1 ngay.
  BEGIN
    MERGE INTO   bk_account_history c
                        USING   (
                        SELECT distinct a.trn_date,a.channel_trn,a.seq_number,a.user_id,a.trf_bank_sk,a.related_person,
                        (case when length(b.account_no) = 13 then LPAD (b.account_no, 14, '0') else TO_CHAR(b.account_no) end) account_no,
                        a.related_account
                                   FROM   dwh.ddft_transaction@dblink_balance a inner join dwh.dddm_account@dblink_balance b
                                on a.account_no_sk = b.account_no_sk
                                where a.channel_trn is not null and trn_date = to_number(to_char(p_date,'yyyyddd'))
                                and a.dorc_ind is not null and (related_person is not null or related_account is not null or trf_bank_sk is not null)
                                and a.trn_sk != 70351 ) src
                           ON   (c.trace_code is not null and c.rollout_acct_no = src.account_no and  to_char(c.tran_time,'yyyyddd') = src.trn_date and c.trace_code = src.channel_trn and c.tm_seq = src.seq_number
                           and trim(c.teller_id) = src.user_id)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.beneficiary_acct_no = trim(src.related_account),
                           c.beneficiary_acct_bank = trim(src.trf_bank_sk),
                           c.beneficiary_acct_name = trim(SUBSTR( src.related_person,1,100));
--                         c.beneficiary_acct_name = trim(src.related_person)

                       commit;
      MERGE INTO   bk_account_history c
      USING   (
      SELECT distinct a.trn_date,a.channel_trn,a.seq_number,a.user_id,a.trf_bank_sk,a.related_person,
      (case when length(b.account_no) = 13 then LPAD (b.account_no, 14, '0') else TO_CHAR(b.account_no) end) account_no,
      a.related_account
                 FROM   dwh.ddft_transaction@dblink_balance a inner join dwh.dddm_account@dblink_balance b
              on a.account_no_sk = b.account_no_sk
              where a.channel_trn is not null and trn_date = to_number(to_char(p_date,'yyyyddd'))
              and a.dorc_ind is not null and (related_person is not null or related_account is not null or trf_bank_sk is not null)
              and a.trn_sk = 70351 ) src
         ON   (c.trace_code is not null and c.rollout_acct_no = src.account_no and  to_char(c.tran_time,'yyyyddd') = src.trn_date and c.trace_code = src.channel_trn and c.tm_seq = src.seq_number
         and trim(c.teller_id) = src.user_id)
 WHEN MATCHED
 THEN
     UPDATE SET
         c.beneficiary_acct_no = trim(src.related_account),
         c.beneficiary_acct_bank = trim(src.trf_bank_sk),
         c.beneficiary_acct_name = trim(src.related_person);
         commit;

  END;






 PROCEDURE proc_card_statement IS
  BEGIN
     insert into card_statement (select * from card_statement_sql);

     commit;
  END;


 PROCEDURE proc_card_payment_bk IS
    sqlstr varchar2(4000);
  BEGIN
    insert into bc_ccard_payment_history_bk (select * from bc_ccard_payment_history
    where create_time >= to_date('01-'||to_char(sysdate,'MM')||'-2014','dd-MM-yyyy') and
    create_time <= to_date('15-'||to_char(sysdate,'MM')||'-2014','dd-MM-yyyy'));

   sqlstr := 'delete bc_ccard_payment_history
    where create_time >= to_date(''01-'||to_char(sysdate,'MM')||'-2014'',''dd-MM-yyyy'') and
   create_time <= to_date(''15-'||to_char(sysdate,'MM')||'-2014'',''dd-MM-yyyy'')';
    execute immediate sqlstr;
    commit;
  END;







END;

/
