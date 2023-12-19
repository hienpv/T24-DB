--------------------------------------------------------
--  DDL for Procedure PR_BENIFIT_INT_ACC_DDHIST
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PR_BENIFIT_INT_ACC_DDHIST" (p_acct_no VARCHAR2, p_date7 NUMBER)
 as

    CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE)
        IS
                SELECT h.rowid h_rowid, his.tracct beneficiary_acct_no,
                      'MARITIME BANK' beneficiary_acct_bank,
                      bka.acct_name beneficiary_acct_name
                FROM ibs.bk_account_history h, STAGING.SI_HIS_DDHIST@STAGING_PRO_CORE his, ibs.cstb_account_info bka
                WHERE 1=1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1 --#20160723 Loctx change
                AND his.trdate = p_cur_date7
                AND TRIM(h.trace_code) = TRIM(his.auxtrc)--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = his.seq -- sequence
                AND TRIM(h.teller_id )= TRIM(his.truser) -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> his.dorc -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND bka.acct_no(+) = his.tracct
                AND h.amount = his.amt --#20170213 Loctx add
                /*--#20160928 Loctx disable: khong can, da filter trong job TMTRANCDC (CDC)
                AND TRIM(tm.tmapptype) IN ('D', 'S', --DD ACCOUNT
                                      'L', 'T') -- LN AND FD
                */
                --AND TRIM(tm.tmtxcd) NOT IN--#20160723 Loctx change
                AND h.trace_code NOT IN--#20160723 Loctx change
                                  ('8121',
                                      '8122',
                                      '8123',
                                      '8124',
                                      '8125',
                                      '8126',
                                      '8161',
                                      '8162',
                                      '8163',
                                      '8171',
                                      '8172',
                                      '8173',
                                      '8176',
                                      '8177',
                                      '8178')
                --AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$';
                AND h.rollout_acct_no = p_acct_no;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);

begin

    OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'));
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history SET beneficiary_acct_no = l_txn_list(i).beneficiary_acct_no,
                                                beneficiary_acct_name = l_txn_list(i).beneficiary_acct_name,
                                                beneficiary_acct_bank = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;

    COMMIT;
end;

/
