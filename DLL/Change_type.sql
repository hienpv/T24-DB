------------Doi kieu du lieu bang bk_account_history cot tm_seq
Create table bk_account_history_pilot
as 
select CORE_SN, TRAN_SN, TRAN_SERVICE_CODE, 
  TRACE_CODE, DC_SIGN, ACCEPTS_ORG, 
  TRAN_TYPE, TRAN_DEVICE, DEVICE_NO, 
  VOUCHER_TYPE, CURRENCY_CODE, ROLLOUT_ACCT_NO, 
  ROLLOUT_ACCT_NAME, ROLLOUT_CARD_NO, 
  BENEFICIARY_ACCT_NO, BENEFICIARY_ACCT_NAME, 
  BENEFICIARY_ACCT_BANK, BENEFICIARY_ACCT_BRANCH, 
  BENEFICIARY_CARD_NO, AMOUNT, FEE, 
  PRE_BALANCE, ACT_BALANCE, POST_TIME, 
  TRAN_TIME, STATUS, OPERATOR, CHANNEL, 
  REMARK, INSERT_DATE, TELLER_ID, TO_CHAR(TM_SEQ) as TM_SEQ, 
  SYNC_TYPE, TC_CODE, TC_SYNC_TIME, 
  LN_TIME, DUE_DATE, OS_BALANCE
from bk_account_history;

commit;

--Thuc hien doi ten bang bk_account_history -> bk_account_history_bk_20231219
--Thuc hien doi ten bang bk_account_history_pilot -> bk_account_history
--Thuc hien đánh lại PARTITION, INDEX ... --> caonv2@msb.com.vn gửi lại script

------------Doi kieu du lieu bang sync_cdc_bk_acct_hist_fail cot tm_seq
--Backup bang
create table sync_cdc_hist_fail_bk_20231219
as select * from sync_cdc_bk_acct_hist_fail;

--Tao cot moi
ALTER TABLE sync_cdc_bk_acct_hist_fail
  ADD tm_seq_bk VARCHAR2(40);
--Update du lieu tu cot cu sang cot moi
update sync_cdc_bk_acct_hist_fail set tm_seq_bk = TO_CHAR(TM_SEQ) where 1= 1;
commit;
--update cot tm_seq = null va thuc hien doi kieu
update sync_cdc_bk_acct_hist_fail set tm_seq = null where 1= 1;
commit;
alter table
   sync_cdc_bk_acct_hist_fail
modify
(
   TM_SEQ    varchar2(40)
);

update sync_cdc_bk_acct_hist_fail set tm_seq = tm_seq_bk where 1= 1;

commit;

------------Doi kieu du lieu bang sync_cdc_bk_acct_hist_fail cot tm_seq
--Backup bang
create table sync_bk_acc_his_bk_20231219
as select * from sync_bk_account_history;

--Tao cot moi
ALTER TABLE sync_bk_account_history
  ADD tm_seq_bk VARCHAR2(40);
--Update du lieu tu cot cu sang cot moi
update sync_bk_account_history set tm_seq_bk = TO_CHAR(TM_SEQ) where 1= 1;
commit;
--update cot tm_seq = null va thuc hien doi kieu
update sync_bk_account_history set tm_seq = null where 1= 1;
commit;
alter table
   sync_bk_account_history
modify
(
   TM_SEQ    varchar2(40)
);

update sync_bk_account_history set tm_seq = tm_seq_bk where 1= 1;

commit;