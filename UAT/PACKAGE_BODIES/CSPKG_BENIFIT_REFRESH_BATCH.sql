--------------------------------------------------------
--  DDL for Package Body CSPKG_BENIFIT_REFRESH_BATCH
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_BENIFIT_REFRESH_BATCH" AS
	
    PROCEDURE pr_benifit_refresh_core_batch
    IS
        CURSOR c_his_247
        IS
            select ROLLOUT_ACCT_NO,
                 AMOUNT,
                 TRAN_TIME,
                 TM_SEQ,
                 BENEFICIARY_ACCT_NO, 
                 BENEFICIARY_ACCT_NAME, 
                 BENEFICIARY_ACCT_BANK
            from bk_account_history_247
            where tran_time >= trunc(sysdate -1)
                  AND TELLER_ID in ('IBSML247', 'VASML247', 'IBSML247P', 'IBDN247')
                  --AND TRACE_CODE = 'NP1321I' 
                  AND BENEFICIARY_ACCT_NO is not null
                  AND BENEFICIARY_ACCT_BANK is not null;

        TYPE l_his_247 IS TABLE OF c_his_247%ROWTYPE;

        l_txn_list l_his_247;

        l_error_desc VARCHAR2(300);

    BEGIN       
        OPEN c_his_247;
        LOOP
            FETCH c_his_247
            BULK COLLECT INTO l_txn_list
            LIMIT  1000;

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history SET BENEFICIARY_ACCT_NO = l_txn_list(i).BENEFICIARY_ACCT_NO,
                                                BENEFICIARY_ACCT_BANK = l_txn_list(i).BENEFICIARY_ACCT_BANK
                WHERE ROLLOUT_ACCT_NO = l_txn_list(i).ROLLOUT_ACCT_NO
                      AND TM_SEQ = l_txn_list(i).TM_SEQ
                      AND TELLER_ID in ('IBSML247', 'VASML247', 'IBSML247P', 'IBDN247')
                      --AND TRACE_CODE = 'NP1321I' 
                      AND AMOUNT = l_txn_list(i).AMOUNT
                      AND BENEFICIARY_ACCT_NO is null
                      AND BENEFICIARY_ACCT_BANK is null
                      AND (to_char(TRAN_TIME, 'ddMMyy') =  to_char(l_txn_list(i).TRAN_TIME, 'ddMMyy')
                          OR to_char(TRAN_TIME - 1, 'ddMMyy') =  to_char(l_txn_list(i).TRAN_TIME, 'ddMMyy'));
            COMMIT;

            EXIT WHEN c_his_247%NOTFOUND;

        END LOOP;
        CLOSE c_his_247 ;

    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            CLOSE c_his_247;
            --forward error
            RAISE;
    END;    
END cspkg_benifit_refresh_batch;

/
