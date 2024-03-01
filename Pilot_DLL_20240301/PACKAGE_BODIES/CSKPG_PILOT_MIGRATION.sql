--------------------------------------------------------
--  DDL for Package Body CSKPG_PILOT_MIGRATION
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSKPG_PILOT_MIGRATION" AS
  
  PROCEDURE cd_mast_migration
    IS  
        CURSOR c_data
        IS
          select acct_no, BRN_T24 
          from bk_account_info ba
          inner join STAGING.SI_DAT_CDMAST@STAGING_PRO_CORE co
          on ba.acct_no = co.acctno
          where ba.ACCT_TYPE = 'FD';
 
       r_row   c_data%ROWTYPE;
       l_check_commit NUMBER := 0;
    BEGIN
        
        OPEN c_data;
        LOOP
          FETCH c_data INTO r_row;
          EXIT WHEN c_data%NOTFOUND;
          l_check_commit := l_check_commit + 1;
          
          update bk_account_info set org_no = r_row.BRN_T24, branch_no = r_row.BRN_T24
          where acct_no = r_row.acct_no;
          
          if MOD(l_check_commit, 10000) = 0
          THEN
            commit;
          end if;

        END LOOP;        
        commit;
        CLOSE c_data;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;            
  END;
  
  PROCEDURE dd_mast_migration
    IS  
        CURSOR c_data
        IS
          select acct_no, BRANCH_T24 
          from bk_account_info ba
          inner join STAGING.SI_DAT_DDMAST@STAGING_PRO_CORE co
          on ba.acct_no = co.acctno
          where ba.ACCT_TYPE = 'CA';
 
       r_row   c_data%ROWTYPE;
       l_check_commit NUMBER := 0;
    BEGIN
        
        OPEN c_data;
        LOOP
          FETCH c_data INTO r_row;
          EXIT WHEN c_data%NOTFOUND;
          l_check_commit := l_check_commit + 1;
          
          update bk_account_info set org_no = r_row.BRANCH_T24, branch_no = r_row.BRANCH_T24
          where acct_no = r_row.acct_no;
          
          if MOD(l_check_commit, 10000) = 0
          THEN
            commit;
          end if;

        END LOOP;        
        commit;
        CLOSE c_data;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;            
  END;
  
  PROCEDURE ln_mast_migration
    IS  
        CURSOR c_data
        IS
          select acct_no, RBRN_T24 
          from bk_account_info ba
          inner join STAGING.SI_DAT_LNMAST@STAGING_PRO_CORE co
          on ba.acct_no = co.acctno
          where ba.ACCT_TYPE = 'LN';
 
       r_row   c_data%ROWTYPE;
       l_check_commit NUMBER := 0;
    BEGIN
        
        OPEN c_data;
        LOOP
          FETCH c_data INTO r_row;
          EXIT WHEN c_data%NOTFOUND;
          l_check_commit := l_check_commit + 1;
          
          update bk_account_info set org_no = r_row.RBRN_T24, branch_no = r_row.RBRN_T24
          where acct_no = r_row.acct_no;
          
          if MOD(l_check_commit, 10000) = 0
          THEN
            commit;
          end if;

        END LOOP;        
        commit;
        CLOSE c_data;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;            
  END;
   PROCEDURE bk_cif_migration
    IS  
    --select b.org_no,a.cfbrnn,a.cfbrnn_t24 from STAGING.SI_DAT_CFMAST@staging_pro_core a , bk_cif b where a.cfcifn=b.cif_no
        CURSOR c_data
        IS
         select b.cif_no,b.org_no,a.cfbrnn,a.cfbrnn_t24 from  bk_cif b inner join STAGING.SI_DAT_CFMAST@staging_pro_core a  on  a.cfcifn=b.cif_no;
         r_row   c_data%ROWTYPE;
       l_check_commit NUMBER := 0;
    BEGIN
        
        OPEN c_data;
        LOOP
          FETCH c_data INTO r_row;
          EXIT WHEN c_data%NOTFOUND;
          l_check_commit := l_check_commit + 1;
          
          update bk_cif set org_no = r_row.cfbrnn_t24
          where cif_no = r_row.cif_no;
          
          if MOD(l_check_commit, 10000) = 0
          THEN
            commit;
          end if;

        END LOOP;        
        commit;
        CLOSE c_data;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;            
  END;
  
  PROCEDURE bk_account_history_change_type
    IS  
        CURSOR c_data
        IS
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
          from bk_account_history_bk_20231219;
       
       TYPE ARRAY IS TABLE OF BK_ACCOUNT_HISTORY_PILOT_NEW%ROWTYPE;
       l_data ARRAY;
       l_check_commit NUMBER := 0;
       p_array_size NUMBER:= 10000;
    BEGIN
        
        OPEN c_data;
        LOOP
          FETCH c_data BULK COLLECT INTO l_data LIMIT p_array_size;
          FORALL i IN 1..l_data.COUNT
            insert into BK_ACCOUNT_HISTORY_PILOT_NEW (CORE_SN, TRAN_SN, TRAN_SERVICE_CODE, 
              TRACE_CODE, DC_SIGN, ACCEPTS_ORG, 
              TRAN_TYPE, TRAN_DEVICE, DEVICE_NO, 
              VOUCHER_TYPE, CURRENCY_CODE, ROLLOUT_ACCT_NO, 
              ROLLOUT_ACCT_NAME, ROLLOUT_CARD_NO, 
              BENEFICIARY_ACCT_NO, BENEFICIARY_ACCT_NAME, 
              BENEFICIARY_ACCT_BANK, BENEFICIARY_ACCT_BRANCH, 
              BENEFICIARY_CARD_NO, AMOUNT, FEE, 
              PRE_BALANCE, ACT_BALANCE, POST_TIME, 
              TRAN_TIME, STATUS, OPERATOR, CHANNEL, 
              REMARK, INSERT_DATE, TELLER_ID, TM_SEQ, 
              SYNC_TYPE, TC_CODE, TC_SYNC_TIME, 
              LN_TIME, DUE_DATE, OS_BALANCE)
            values( l_data(i).CORE_SN, l_data(i).TRAN_SN, l_data(i).TRAN_SERVICE_CODE, 
              l_data(i).TRACE_CODE, l_data(i).DC_SIGN, l_data(i).ACCEPTS_ORG, 
              l_data(i).TRAN_TYPE, l_data(i).TRAN_DEVICE, l_data(i).DEVICE_NO, 
              l_data(i).VOUCHER_TYPE, l_data(i).CURRENCY_CODE, l_data(i).ROLLOUT_ACCT_NO, 
              l_data(i).ROLLOUT_ACCT_NAME, l_data(i).ROLLOUT_CARD_NO, 
              l_data(i).BENEFICIARY_ACCT_NO, l_data(i).BENEFICIARY_ACCT_NAME, 
              l_data(i).BENEFICIARY_ACCT_BANK, l_data(i).BENEFICIARY_ACCT_BRANCH, 
              l_data(i).BENEFICIARY_CARD_NO, l_data(i).AMOUNT, l_data(i).FEE, 
              l_data(i).PRE_BALANCE, l_data(i).ACT_BALANCE, l_data(i).POST_TIME, 
              l_data(i).TRAN_TIME, l_data(i).STATUS, l_data(i).OPERATOR, l_data(i).CHANNEL, 
              l_data(i).REMARK, l_data(i).INSERT_DATE, l_data(i).TELLER_ID, l_data(i).TM_SEQ, 
              l_data(i).SYNC_TYPE, l_data(i).TC_CODE, l_data(i).TC_SYNC_TIME, 
              l_data(i).LN_TIME, l_data(i).DUE_DATE, l_data(i).OS_BALANCE);
      
          EXIT WHEN c_data%NOTFOUND;
          commit;
        END LOOP;        
        commit;
        CLOSE c_data;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;            
  END;
  
END CSKPG_PILOT_MIGRATION;

/
