--------------------------------------------------------
--  DDL for Package Body CSPKG_BENIFIT_REFRESH_247
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."CSPKG_BENIFIT_REFRESH_247" AS

    PROCEDURE pr_benifit_internal_txn
    IS
        CURSOR c_txn (p_batch_no NUMBER)
        IS
                SELECT h.row_id h_rowid, LPAD(tm.rollout_acct_no, 14, '0') beneficiary_acct_no,
                        'MARITIME BANK' beneficiary_acct_bank,
                        bka.acct_name beneficiary_acct_name
                FROM twtb_acc_history_benifit_247 h, BK_ACCOUNT_HISTORY_TMTR24 tm, cstb_account_info bka
                WHERE 1=1
                AND h.batch_no = p_batch_no
                AND h.tran_time >= trunc(sysdate) AND h.tran_time < trunc(sysdate+1) --#20160723 Loctx change
                AND tm.tmentdt7 = to_number(to_char(sysdate+1/4, 'RRRRDDD'))
                AND NVL(h.trace_code, '$X$') = NVL(tm.trace_code, '$X$')--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = tm.tm_seq -- sequence
                AND h.teller_id = trim(tm.teller_id) -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> tm.dc_sign -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.amount = tm.amount --#20170214 Loctx add

                --#20171223 Loctx add
                --02. join de xac dinh giao dich doi ung, fix loi theo yeu cau cua MSB
                AND TO_NUMBER(TO_CHAR(h.tran_time, 'hh24miss')) = tm.tmtiment
                --#20171223 Loctx add end

                AND bka.acct_no(+) = tm.rollout_acct_no
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
                                        '8178','EB9615')
        AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$')
        ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);
        l_batch_no NUMBER(20);

    BEGIN
        SELECT TO_CHAR(SYSDATE, 'RRRRMMDD') || lpad(seq_batch_benifit_247.nextval, 10, '0')
        INTO l_batch_no
        FROM dual;

        pr_load_tw_txn(l_batch_no);

        OPEN c_txn(l_batch_no);
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  1000;

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history_247 SET beneficiary_acct_no = l_txn_list(i).beneficiary_acct_no,
                                                beneficiary_acct_name = l_txn_list(i).beneficiary_acct_name,
                                                beneficiary_acct_bank = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;

        pr_unload_tw_txn(l_batch_no);

    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            pr_unload_tw_txn(l_batch_no);
            l_error_desc := substr(SQLERRM, 200);
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

    PROCEDURE pr_benifit_internal_txn_on_day
    IS
	BEGIN
        pr_benifit_internal_txn();

    END;

    PROCEDURE pr_load_tw_txn(p_batch_no NUMBER)--#20170214 lOCTX ADD FOR TUNING
    IS
    BEGIN
        INSERT INTO twtb_acc_history_benifit_247 c
                                    (batch_no,
                                    row_id,
                                    core_sn,
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
                                     teller_id,
                                     tm_seq,
                                     sync_type,
                                     status,
                                     tc_code,
                                     tran_sn,
                                     tran_service_code,
                                     tran_device,
                                     device_no,
                                     trace_code)
        SELECT   p_batch_no,
                ROWID,
                h.core_sn,
                                     h.tran_time,
                                     h.post_time,
                                     h.dc_sign,
                                     h.amount,
                                     h.currency_code,
                                     h.pre_balance,
                                     h.channel,
                                     h.remark,
                                     h.rollout_acct_no,
                                     h.insert_date,
                                     h.teller_id,
                                     h.tm_seq,
                                     h.sync_type,
                                     h.status,
                                     h.tc_code,
                                     h.tran_sn,
                                     h.tran_service_code,
                                     h.tran_device,
                                     h.device_no,
                                     h.trace_code
        FROM bk_account_history_247 h
        WHERE h.tran_time >= trunc(sysdate) AND h.tran_time < trunc(sysdate+1)
        AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$');
        commit;
    END;

	PROCEDURE pr_unload_tw_txn(p_batch_no NUMBER)--#20170214 lOCTX ADD FOR TUNING
    IS
    BEGIN

        DELETE FROM twtb_acc_history_benifit_247 --#20170214 Loctx add
        WHERE batch_no = p_batch_no;

        COMMIT;
    END;

END cspkg_benifit_refresh_247;

/
