--------------------------------------------------------
--  DDL for Package Body PK_UPDATE_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_UPDATE_DATA" AS

  PROCEDURE SYNC_DATA_SWIFT_INFO AS
  BEGIN
    -- Kiem tra data tren co tren CORE ma khong co trong BK_SWIFT_INFO thi insert vao
    FOR data_rec IN (
      select trim(a.SWBCOD) SWBCOD, trim(max(a.SWBNAM)) SWBNAM, trim(max(a.SWBAD1)) SWBAD1, trim(max(a.SWCONT)) SWCONT
      from SVPARPV51.SWBICD@DBLINK_DATA a
      left join BK_SWIFT_INFO b on trim(a.SWBCOD) = b.SWIFT_CODE
      where b.SWIFT_CODE is null
      group by trim(a.SWBCOD)
    )
    LOOP
      INSERT INTO BK_SWIFT_INFO (SWIFT_CODE,SWIFT_NAME,SWIFT_ADD,SWIFT_REGION)
      VALUES(data_rec.SWBCOD, data_rec.SWBNAM, data_rec.SWBAD1, data_rec.SWCONT);
    END LOOP;
    commit;
    -- Co data trong bang BK_SWIFT_INFO ma khong co tren CORE thi xoa di
    delete from BK_SWIFT_INFO where SWIFT_CODE IN (
      select a.SWIFT_CODE
      from BK_SWIFT_INFO a
      left join SVPARPV51.SWBICD@DBLINK_DATA b on trim(b.SWBCOD) = a.SWIFT_CODE
      where b.SWBCOD is null
    );
    commit;
  END SYNC_DATA_SWIFT_INFO;

  PROCEDURE SYNC_DATA_BENEFIT_RETAIL (
    P_ROLLOUT_ACCT_NO VARCHAR2
  )
  AS
  BEGIN
--    FOR data_rec IN (
----      select bth.amount, bth.create_time, bth.beneficiary_account_no as benefit_account, bth.beneficiary_name as benefit_name, bth.beneficiary_bank_name as benefit_bank, 
----			ssb.tc_code, ssb.tc_code2, bth.rollout_account_no as rollout_account, ssb.tran_sn, bth.is_inter_bank, bth.beneficiary_branch_name, bth.core_sn
----      from bk_sys_sml_benefit ssb
----      left join bc_transfer_history bth on ssb.tran_sn = bth.tran_sn
----      where ssb.tc_code is null 
----      -- and ssb.tran_sn not like '' || to_char(sysdate, 'yyyyMMdd')|| '%'
----      and bth.ROLLOUT_ACCOUNT_NO=P_ROLLOUT_ACCT_NO
--      
--      select bth.amount, bth.create_time, bth.beneficiary_account_no as benefit_account, bth.beneficiary_name as benefit_name, bth.beneficiary_bank_name as benefit_bank, 
--			bth.rollout_account_no as rollout_account, bth.tran_sn, bth.is_inter_bank, bth.beneficiary_branch_name, bth.core_sn
--      from bc_transfer_history bth
--      where bth.ROLLOUT_ACCOUNT_NO=P_ROLLOUT_ACCT_NO and bth.UPDATE_TIME >= '1 AUG 2021'
--       
--    )  
--    LOOP
--      update bk_account_history 
--      set 
--        beneficiary_acct_no= data_rec.benefit_account,
--        beneficiary_acct_name= data_rec.benefit_name,
--        beneficiary_acct_bank= data_rec.benefit_bank,
--        beneficiary_acct_branch= data_rec.beneficiary_branch_name
--      where rollout_acct_no=data_rec.rollout_account
--      and tm_seq= data_rec.core_sn
--      and amount=data_rec.amount
--      and trunc(tran_time) >= trunc(data_rec.create_time);
--      
--      -- update bk_sys_sml_benefit set tc_code='-1' where tran_sn=data_rec.tran_sn;
--    END LOOP;
--    commit;
    null;
  END;
END PK_UPDATE_DATA;

/
