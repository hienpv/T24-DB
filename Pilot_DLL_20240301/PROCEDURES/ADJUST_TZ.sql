--------------------------------------------------------
--  DDL for Procedure ADJUST_TZ
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "ADJUST_TZ" 
 as

begin
    /*UPDATE ibs.bk_account_history a
    SET (a.beneficiary_acct_no,
        a.beneficiary_acct_name,
        a.beneficiary_acct_bank)
        =
        (SELECT a.corr_account_no,
                a.corr_account_name,
                a.corr_bank_name
        FROM txtb_Transaction@z_ods_pro a
        WHERE a.account_id = 1267115
        AND a.txn_date7 = 2016217
        AND a.txn_seq_in_source = 317321047)
    WHERE a.rollout_acct_no = '03101011807493'
    AND TRUNC(a.tran_time) = TO_DATE('04082016', 'DDMMRRRR')
    AND a.tm_seq = 317321047;*/

    /*UPDATE ibs.bk_account_history a
    SET a.beneficiary_acct_bank = NULL,
        a.beneficiary_acct_no = NULL,
        a.beneficiary_acct_name = NULL
    WHERE a.rollout_acct_no = '24001010556666'
    AND TRUNC(a.tran_time) = TO_DATE(20161227, 'RRRRMMDD')
    AND a.amount = 15500000;*/

    /*UPDATE ibs.bk_account_history a
    SET (a.beneficiary_acct_no,
        a.beneficiary_acct_name,
        a.beneficiary_acct_bank)
        =
        (SELECT b.beneficiary_acct_no,
                b.beneficiary_acct_name,
                b.beneficiary_acct_bank
        FROM cdc.z_quan_bk_acct_his_2016361 b
        WHERE 1=1
        AND a.rowid = b.h_rowid)
    WHERE 1=1
    AND a.tran_time >= TO_DATE(20161226, 'RRRRMMDD')
    AND a.tran_time < TO_DATE(20161226, 'RRRRMMDD') + 1
    AND EXISTS (SELECT 1
                FROM cdc.z_quan_bk_acct_his_2016361 c
                WHERE c.h_rowid = a.rowid
                AND NVL(a.beneficiary_acct_no, 0) <> NVL(c.beneficiary_acct_no, 0))
    AND a.rollout_acct_no = '04001010704571';*/

    /*FOR i IN
        (
            SELECT a.rowid, a.rollout_acct_no, a.amount, a.beneficiary_acct_no, a.beneficiary_acct_name, a.beneficiary_acct_bank,
                    b.corr_account_no new_acct_no, b.corr_account_name new_acct_name, b.corr_bank_name new_bank
            FROM ibs.bk_account_history a, ods.txtb_transaction@z_ods_pro b, ods.actb_account@z_ods_pro c
            WHERE a.rollout_acct_no = '15001010005667'
            AND b.account_id = c.account_id
            AND LPAD(a.rollout_acct_no, 14, '0') = LPAD(c.account_no, 14, '0')
            AND a.tran_time >= TO_DATE(b.txn_date7, 'RRRRDDD')
            AND a.tran_time < TO_DATE(b.txn_date7, 'RRRRDDD') + 1
            AND a.tm_seq = b.txn_seq_in_source
            AND a.dc_sign = b.dorc_ind
        )
    LOOP
        UPDATE bk_account_history h
        SET h.beneficiary_acct_no = i.new_acct_no,
            h.beneficiary_acct_name = i.new_acct_name,
            h.beneficiary_acct_bank = i.new_bank
        WHERE h.rowid = i.rowid;
    END LOOP;*/

    COMMIT;
end;

/
