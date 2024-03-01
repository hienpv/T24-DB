--------------------------------------------------------
--  DDL for Package Body CSPKG_BENIFIT_REFRESH_MANUAL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."CSPKG_BENIFIT_REFRESH_MANUAL" 
  IS
 /*----------------------------------------------------------------------------------------------------
     ** Module   : COMMODITY SYSTEM
     ** and is copyrighted by FSS.
     **
     **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
     **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
     **    graphic, optic recording or otherwise, translated in any language or computer language,
     **    without the prior written permission of Financial Software Solutions. JSC.
     **
     **  MODIFICATION HISTORY
     **  Person      Date           Comments
     **  LocTX      02-12-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/

    pkgctx    plog.log_ctx;
    logrow    tlogdebug%ROWTYPE;
    PROCEDURE pr_sync_benifit_from_ods_acc(p_date number, p_acc varchar) 
    IS
    BEGIN
        --- back kup
--        delete  Z_TEMP_BK_ACCOUNT_HISTORY_NEW2  news
--        where  news.tran_time>= TO_DATE(p_date , 'yyyyddd')  
--          and news.tran_time < TO_DATE(p_date , 'yyyyddd')  + 1
--          and news.ROLLOUT_ACCT_NO  = p_acc;
          
--        insert into Z_TEMP_BK_ACCOUNT_HISTORY_NEW2
--        select * from IBS.BK_ACCOUNT_HISTORY OLDS 
--        where  OLDS.tran_time>= TO_DATE(p_date , 'yyyyddd')  
--          and OLDS.tran_time < TO_DATE(p_date , 'yyyyddd')  + 1 
--          and OLDS.ROLLOUT_ACCT_NO  = p_acc;
        
        
--         delete z_update_benefit_ods where post_time7 = p_date  and rollout_acct_no =p_acc ;
  
         insert into z_update_benefit_ods
         SELECT AC.ACCOUNT_NO rollout_acct_no, trim(A.maker_code) teller_id, a.txn_seq_in_source tm_seq, 
                txn_date7 post_time7,  
                TO_DATE((txn_date7 || ':' ||
                                                    LPAD(TXN_TIME,
                                                          6,
                                                          '0')),
                                                    'yyyyddd:hh24miss') post_time,
                A.dorc_ind dc_sign, M.txn_code device_no,
                A.CORR_ACCOUNT_NO, A.CORR_ACCOUNT_NAME, A.CORR_BANK_NAME,
                a.amount_acy amount
          FROM txtb_transaction@z_ods_pro a, cstb_transaction_code@z_ods_pro M , ACTB_ACCOUNT@z_ods_pro AC
          WHERE A.txn_code_id = M.txn_code_id
            AND A.ACCOUNT_ID = AC.ACCOUNT_ID
            and txn_date7 = p_date 
            and AC.ACCOUNT_NO =p_acc
            and A.CORR_ACCOUNT_NO is not null ;
     
        merge into IBS.BK_ACCOUNT_HISTORY A
        using (
                select X.rollout_acct_no,
                       x.tran_time,
                       x.post_time,
                       x.dc_sign,
                       x.device_no, 
                       x.teller_id,
                       x.tm_seq,
                       x.amount, 
                       X.BENEFICIARY_ACCT_NO, 
                       X.BENEFICIARY_ACCT_NAME, 
                       X.BENEFICIARY_ACCT_BANK,
                       Y.CORR_ACCOUNT_NO,
                       Y.CORR_ACCOUNT_NAME,
                       Y.CORR_BANK_NAME
                from IBS.BK_ACCOUNT_HISTORY x,
                z_update_benefit_ods y 
                where  x.tran_time>= TO_DATE(p_date , 'yyyyddd')  
                and x.tran_time < TO_DATE(p_date , 'yyyyddd')  + 1
                and X.rollout_acct_no = y.rollout_acct_no 
                and x.post_time = y.post_time 
                and x.dc_sign = y.dc_sign
                and x.device_no = y.device_no
                and x.teller_id = trim(y.teller_id)
                and x.tm_seq  = y.tm_seq
                and x.amount = y.amount
                and X.ROLLOUT_ACCT_NO = p_acc
                AND (NVL(x.beneficiary_acct_bank, '$NULL$') = '$NULL$' 
                OR NVL(x.beneficiary_acct_name, '$NULL$') = '$NULL$'
                OR NVL(x.beneficiary_acct_no, '$NULL$') = '$NULL$') 
               ) B   on (
                       A.rollout_acct_no = b.rollout_acct_no
                       And A.tran_time  = b.tran_time
                       AND A.post_time = B.post_time
                       And A.dc_sign = b.dc_sign
                       And A.device_no = b.device_no 
                       AND A.teller_id = b.teller_id
                       AND A.tm_seq = b.tm_seq
                       AND A.amount = b.amount 
        ) WHEN MATCHED then 
                update set A.BENEFICIARY_ACCT_NO = B.CORR_ACCOUNT_NO,
                           A.BENEFICIARY_ACCT_NAME   = B.CORR_ACCOUNT_NAME,
                           A.BENEFICIARY_ACCT_BANK = B.CORR_BANK_NAME ;
    COMMIT;

    EXCEPTION
        when others then 
            rollback; 
            
        RAISE;           
                      
    END;
    PROCEDURE pr_sync_benifit_from_ods(p_date number) 
    IS
    v_sql clob ;
    BEGIN
        --- back kup
        delete  Z_TEMP_BK_ACCOUNT_HISTORY_NEW  news
        where  news.tran_time>= TO_DATE(p_date , 'yyyyddd')  
          and news.tran_time < TO_DATE(p_date , 'yyyyddd')  + 1 ;
          
        insert into Z_TEMP_BK_ACCOUNT_HISTORY_NEW
        select * from IBS.BK_ACCOUNT_HISTORY OLDS 
        where  OLDS.tran_time>= TO_DATE(p_date , 'yyyyddd')  
          and OLDS.tran_time < TO_DATE(p_date , 'yyyyddd')  + 1 ;
        
        
        EXECUTE IMMEDIATE 'drop table z_update_benefit_ods';

        v_sql:= 'create table z_update_benefit_ods
        as
            SELECT AC.ACCOUNT_NO rollout_acct_no, trim(A.maker_code) teller_id, a.txn_seq_in_source tm_seq, 
                txn_date7 post_time7,  
                TO_DATE((txn_date7 || '':'' ||
                                                    LPAD(TXN_TIME,
                                                          6,
                                                          ''0'')),
                                                    ''yyyyddd:hh24miss'') post_time,
                A.dorc_ind dc_sign, M.txn_code device_no,
                A.CORR_ACCOUNT_NO, A.CORR_ACCOUNT_NAME, A.CORR_BANK_NAME,
                a.amount_acy amount
            FROM txtb_transaction@z_ods_pro a, cstb_transaction_code@z_ods_pro M , ACTB_ACCOUNT@z_ods_pro AC
            WHERE A.txn_code_id = M.txn_code_id
            AND A.ACCOUNT_ID = AC.ACCOUNT_ID
            AND A.CORR_ACCOUNT_NO is not null
            '||'and txn_date7 = '||p_date   ;

        EXECUTE IMMEDIATE v_sql;
          
        merge into IBS.BK_ACCOUNT_HISTORY A
        using (
                select X.rollout_acct_no,
                       x.tran_time,
                       x.post_time,
                       x.dc_sign,
                       x.device_no, 
                       x.teller_id,
                       x.tm_seq,
                       x.amount, 
                       X.BENEFICIARY_ACCT_NO, 
                       X.BENEFICIARY_ACCT_NAME, 
                       X.BENEFICIARY_ACCT_BANK,
                       Y.CORR_ACCOUNT_NO,
                       Y.CORR_ACCOUNT_NAME,
                       Y.CORR_BANK_NAME
                from IBS.BK_ACCOUNT_HISTORY x,
                z_update_benefit_ods y 
                where  x.tran_time>= TO_DATE(p_date , 'yyyyddd')  
                and x.tran_time < TO_DATE(p_date , 'yyyyddd')  + 1
                and X.rollout_acct_no = y.rollout_acct_no 
                and x.post_time = y.post_time 
                and x.dc_sign = y.dc_sign
                and x.device_no = y.device_no
                and x.teller_id = trim(y.teller_id)
                and x.tm_seq  = y.tm_seq
                and x.amount = y.amount
                AND (NVL(x.beneficiary_acct_bank, '$NULL$') = '$NULL$' 
                OR NVL(x.beneficiary_acct_name, '$NULL$') = '$NULL$'
                OR NVL(x.beneficiary_acct_no, '$NULL$') = '$NULL$') 
               ) B   on (
                       A.rollout_acct_no = b.rollout_acct_no
                       And A.tran_time  = b.tran_time
                       AND A.post_time = B.post_time
                       And A.dc_sign = b.dc_sign
                       And A.device_no = b.device_no 
                       AND A.teller_id = b.teller_id
                       AND A.tm_seq = b.tm_seq
                       AND A.amount = b.amount 
        ) WHEN MATCHED then 
                update set A.BENEFICIARY_ACCT_NO = B.CORR_ACCOUNT_NO,
                           A.BENEFICIARY_ACCT_NAME   = B.CORR_ACCOUNT_NAME,
                           A.BENEFICIARY_ACCT_BANK = B.CORR_BANK_NAME ;
    COMMIT;

    EXCEPTION
        when others then 
            rollback;            
                      
    END;
    
    PROCEDURE pr_load_tw_txn(p_batch_no NUMBER, p_date DATE)--#20170214 lOCTX ADD FOR TUNING
    IS
    BEGIN
        INSERT INTO twtb_account_history_benifit c
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
        FROM bk_account_history h
        WHERE h.tran_time >= p_date AND h.tran_time < p_date + 1
        /*
        AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$')
        */
        ;
        commit;
    END;

    PROCEDURE pr_unload_tw_txn(p_batch_no NUMBER)--#20170214 lOCTX ADD FOR TUNING
    IS
    BEGIN
        DELETE FROM twtb_account_history_benifit --#20170214 Loctx add
        WHERE batch_no = p_batch_no;

        COMMIT;
    END;


    PROCEDURE pr_benifit_internal_by_account(p_date7 NUMBER, p_account_no VARCHAR2)--#20161229 Loctx add
    IS

        CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE)
        IS
                SELECT h.rowid h_rowid, tm.tmacctno beneficiary_acct_no,
                        'MARITIME BANK' beneficiary_acct_bank,
                        bka.acct_name beneficiary_acct_name
                FROM bk_account_history h,
                sync_cdc_tmtran hdd,
                sync_cdc_tmtran tm,
                cstb_account_info bka
                WHERE 1=1
                and h.rollout_acct_no = p_account_no
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1 --#20160723 Loctx change

                --#20171223 Loctx add
                --01. join de xac dinh TRTKTN
                AND hdd.tmentdt7 = p_cur_date7
                AND hdd.tmacctno = TO_NUMBER(p_account_no)--TO_NUMBER(h.rollout_acct_no)
                AND TRIM(hdd.tmdorc) = h.dc_sign
                AND hdd.tmtxseq = h.tm_seq
                AND TRIM(hdd.tmhosttxcd) = h.device_no
                AND TRIM(hdd.tmtellid) = TRIM(h.teller_id)
                AND NVL(TRIM(hdd.tmtxcd), '$X$') = NVL(h.trace_code, '$X$')
                AND hdd.tmtxamt = h.amount
                --02. join de xac dinh giao dich doi ung, fix loi theo yeu cau cua MSB
                AND TRIM(hdd.TmTKTN) = TRIM(tm.TmTKTN)
                AND hdd.tmtiment = tm.tmtiment
                AND TO_NUMBER(TO_CHAR(h.tran_time, 'hh24miss')) = TM.tmtiment
                --#20171223 Loctx add end

                AND tm.tmentdt7 = p_cur_date7
                AND NVL(h.trace_code, '$X$') = NVL(tm.tmtxcd,'$X$') --channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = tm.tmtxseq -- sequence
                AND h.teller_id = tm.tmtellid -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> tm.tmdorc -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.amount = tm.tmtxamt --#20170214 Loctx add
                AND bka.acct_no(+) = tm.tmacctno
                /*--#20160928 Loctx disable: khong can, da filter trong job TMTRANCDC (CDC)
                AND TRIM(tm.tmapptype) IN ('D', 'S', --DD ACCOUNT
                                        'L', 'T') -- LN AND FD
                 */
                --AND TRIM(tm.tmtxcd) NOT IN--#20160723 Loctx change
                AND (h.trace_code NOT IN--#20160723 Loctx change
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
                    OR h.trace_code IS NULL
                                        )

                --AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
                ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_internal_txn' );

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
        plog.setendsection( pkgctx, 'pr_benifit_internal_txn' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_internal_txn' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

    PROCEDURE pr_benifit_internal_act_ddhist(p_date7 NUMBER, p_account_no VARCHAR2)--#20161229 Loctx add
    IS

        CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE)
        IS
                SELECT h.rowid h_rowid, ddhis.tracct beneficiary_acct_no,
                        'MARITIME BANK' beneficiary_acct_bank,
                        bka.acct_name beneficiary_acct_name
                FROM bk_account_history h,
                hstaging.si_his_ddhist@HSTG_PROD_MSB_READ hdd,
                hstaging.si_his_ddhist@HSTG_PROD_MSB_READ ddhis, cstb_account_info bka
                WHERE 1=1
                and h.rollout_acct_no = p_account_no
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1 --#20160723 Loctx change

                --#20171223 Loctx add
                --01. join de xac dinh TRTKTN
                AND hdd.trdate = p_cur_date7
                AND hdd.tracct = TO_NUMBER(p_account_no)--TO_NUMBER(h.rollout_acct_no)
                AND TRIM(hdd.dorc) = h.dc_sign
                AND hdd.seq = h.tm_seq
                AND TRIM(hdd.trancd) = h.device_no
                AND TRIM(hdd.truser) = h.teller_id
                AND NVL(TRIM(hdd.auxtrc), '$X$') = NVL(h.trace_code, '$X$')
                AND hdd.amt = h.amount
                --02. join de xac dinh giao dich doi ung, fix loi theo yeu cau cua MSB
                AND TRIM(hdd.TRTKTN) = TRIM(ddhis.TRTKTN)
                AND hdd.trtime = ddhis.trtime
                AND TO_NUMBER(TO_CHAR(h.tran_time, 'hh24miss')) = ddhis.trtime
                --#20171223 Loctx add end

                AND ddhis.trdate = p_cur_date7
                AND NVL(h.trace_code, '$X$') = NVL(TRIM(ddhis.auxtrc), '$X$')--tm.tmtxcd--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = ddhis.seq -- tm.tmtxseq -- sequence
                AND h.teller_id = TRIM(ddhis.truser)--tm.tmtellid -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> TRIM (ddhis.dorc)--tm.tmdorc -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.amount = ddhis.amt --#20170214 Loctx add
                AND bka.acct_no(+) = ddhis.tracct--tm.tmacctno
                /*--#20160928 Loctx disable: khong can, da filter trong job TMTRANCDC (CDC)
                AND TRIM(tm.tmapptype) IN ('D', 'S', --DD ACCOUNT
                                        'L', 'T') -- LN AND FD
                 */
                --AND TRIM(tm.tmtxcd) NOT IN--#20160723 Loctx change
                AND (h.trace_code NOT IN--#20160723 Loctx change
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
                    OR h.trace_code IS NULL
                                        )
        AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$');


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_internal_txn' );

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
        plog.setendsection( pkgctx, 'pr_benifit_internal_txn' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_internal_txn' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

    PROCEDURE pr_benifit_internal_txn(p_date7 NUMBER)
    IS

        CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE, p_batch_no NUMBER)
        IS
                SELECT h.row_id h_rowid, ddhis.tracct beneficiary_acct_no,
                        'MARITIME BANK' beneficiary_acct_bank,
                        bka.acct_name beneficiary_acct_name
                FROM twtb_account_history_benifit h,
                hstaging.si_his_ddhist@HSTG_PROD_MSB_READ hdd,
                hstaging.si_his_ddhist@HSTG_PROD_MSB_READ ddhis,
                cstb_account_info bka
                WHERE 1=1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.batch_NO = p_batch_no
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1 --#20160723 Loctx change
                --#20171223 Loctx add
                --01. join de xac dinh TRTKTN
                AND hdd.trdate = p_cur_date7
                AND hdd.tracct = TO_NUMBER(h.rollout_acct_no)
                AND TRIM(hdd.dorc) = h.dc_sign
                AND hdd.seq = h.tm_seq
                AND TRIM(hdd.trancd) = h.device_no
                AND TRIM(hdd.truser) = h.teller_id
                AND NVL(TRIM(hdd.auxtrc), '$X$') = NVL(h.trace_code, '$X$')
                AND hdd.amt = h.amount
                --02. join de xac dinh giao dich doi ung, fix loi theo yeu cau cua MSB
                AND TRIM(hdd.TRTKTN) = TRIM(ddhis.TRTKTN)
                AND hdd.trtime = ddhis.trtime
                AND TO_NUMBER(TO_CHAR(h.tran_time, 'hh24miss')) = ddhis.trtime
                --#20171223 Loctx add end

                /*
                AND tm.tmentdt7 = p_cur_date7
                AND h.trace_code = tm.tmtxcd--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = tm.tmtxseq -- sequence
                AND h.teller_id = tm.tmtellid -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> tm.tmdorc -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.amount = tm.tmtxamt --#20170214 Loctx add
                AND bka.acct_no(+) = tm.tmacctno
                */
                AND ddhis.trdate = p_cur_date7
                AND NVL(h.trace_code, '$X$') = NVL(TRIM(ddhis.auxtrc), '$X$')--tm.tmtxcd--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = ddhis.seq -- tm.tmtxseq -- sequence
                AND h.teller_id = TRIM(ddhis.truser)--tm.tmtellid -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> TRIM (ddhis.dorc)--tm.tmdorc -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.amount = ddhis.amt --#20170214 Loctx add
                AND bka.acct_no(+) = ddhis.tracct--tm.tmacctno

                /*--#20160928 Loctx disable: khong can, da filter trong job TMTRANCDC (CDC)
                AND TRIM(tm.tmapptype) IN ('D', 'S', --DD ACCOUNT
                                        'L', 'T') -- LN AND FD
                 */
                --AND TRIM(tm.tmtxcd) NOT IN--#20160723 Loctx change
                AND (h.trace_code NOT IN--#20160723 Loctx change
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
                    OR h.trace_code IS NULL
                                        )
        /*AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$')*/;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);
        l_batch_no NUMBER(20);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_internal_txn' );

        SELECT TO_CHAR(SYSDATE, 'RRRRMMDD') || lpad(ibs.seq_batch_benifit_id.nextval, 10, '0')
        INTO l_batch_no
        FROM dual;

        pr_load_tw_txn(l_batch_no, TO_DATE(p_date7, 'RRRRDDD'));
        dbms_output.put_line ('l_batch_no=' || l_batch_no);

        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'), l_batch_no);
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

        pr_unload_tw_txn(l_batch_no);

        plog.setendsection( pkgctx, 'pr_benifit_internal_txn' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            pr_unload_tw_txn(l_batch_no);

            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_internal_txn' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

    procedure pr_benifit_vcb_out(p_date7 NUMBER)
    IS
        CURSOR c_txn(p_cur_date7 NUMBER, p_cur_date_dt DATE)
                IS
        SELECT  DISTINCT h.rowid AS a_rowid, --#20160928 Loctx change from MIN
                (msg.recieve_name) AS beneficiary_acct_name,
                (msg.receive_account) AS beneficiary_acct_no,
                'EXTERNAL.VCB' AS trf_sys,
                (NVL (msg.banks, 'VCB-SIBS')) AS branch_name
          FROM bk_account_history h, sync_cdc_rmdetl rd, sync_cdc_vcb_msg_content msg
         WHERE     rd.rdjdat = p_cur_date7
               --AND TRUNC(h.post_time) = p_cur_date_dt
               AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
               AND rd.rdauxt = h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
               AND rd.rduser = TRIM(h.teller_id)--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD, #20181221 lOCTX BO SUNG TRIM 
               AND rd.rdseq = h.tm_seq
               AND rd.rdrtyp <> 'G'
               AND rd.rdract = h.rollout_acct_no --#20150116 Loctx add for map giao dich  -- khong can AND rd.rdamt = h.amount vi co tmseq > 0
               --AND CAST (msg.rm_number AS NUMBER (18)) = rd.rdacct --#20150418 Loctx fix loi nham truong (cu: rdract)
               AND msg.rm_number = rd.rdacct--#20150527 Loctx chuyen rm_number thanh number
               AND msg.msg_direction = 'SIBS-VCB'
               AND h.channel NOT IN
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
               AND NVL (h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
               --GROUP BY h.ROWID--#20160928 Loctx change from MIN
               ;

        TYPE type_vcb_tb IS TABLE OF c_txn%ROWTYPE;

        l_vcb_list type_vcb_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_vcb_out' );
        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'));

        LOOP
            FETCH c_txn
              BULK COLLECT INTO l_vcb_list
            LIMIT 10000;

            FORALL i IN 1 .. l_vcb_list.COUNT
                UPDATE bk_account_history c
                   SET c.beneficiary_acct_no = l_vcb_list (i).beneficiary_acct_no,
                       c.beneficiary_acct_name =
                           l_vcb_list (i).beneficiary_acct_name,
                       c.beneficiary_acct_bank = l_vcb_list (i).branch_name
                 WHERE ROWID = l_vcb_list (i).a_rowid;

            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;
        END LOOP;

        CLOSE c_txn ;
        plog.setendsection( pkgctx, 'pr_benifit_vcb_out' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_vcb_out' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

    PROCEDURE pr_benifit_vcb_in(p_date7 NUMBER)
    IS
        CURSOR c_txn(p_cur_date7 NUMBER, p_cur_date_dt DATE)
        IS
            SELECT DISTINCT h.ROWID AS a_rowid,
                   (msg.send_name) AS beneficiary_acct_name,--#20160928 Loctx change from MIN
                   (msg.send_account) AS beneficiary_acct_no,
                   (NVL (msg.banks, 'VCB-SIBS')) AS branch_name
              FROM bk_account_history h,
                   sync_cdc_rmmast rm,
                   sync_cdc_rmdetl rd,
                   sync_cdc_vcb_msg_content msg
             WHERE rd.rdacct = rm.rmacno
                   AND rd.rdjdat = p_cur_date7
                   --AND TRUNC(h.post_time) = p_cur_date_dt
                   AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                   AND rd.rdauxt = h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                   AND rd.rduser = TRIM(h.teller_id)--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD, #20181221 lOCTX BO SUNG TRIM 
                   AND rd.rdseq = h.tm_seq
                   AND rd.rdrtyp <> 'G'
                   AND rd.rdract = h.rollout_acct_no --#20150116 Loctx add for map giao dich  -- khong can AND rd.rdamt = h.amount vi co tmseq > 0
                   AND TRIM(rm.rmref) IS NOT NULL --#20150416 Loctx add for tuning
                   --AND TRIM (msg.f20) =--#20150527 lOCTX BO TRIM
                   AND msg.f20 =
                           DECODE (SUBSTR (rm.rmref, 1, 1),
                                   '0', SUBSTR (rm.rmref, 2),
                                   rm.rmref)
                   AND msg.msg_direction = 'VCB-SIBS'
                   AND h.channel NOT IN
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
                    AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
                    --GROUP BY h.ROWID
                    --#20160928 Loctx change from MIN
                    ;

        TYPE type_vcb_tb IS TABLE OF c_txn%ROWTYPE;

        l_vcb_list type_vcb_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_vcb_in' );

        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'));

        LOOP
            FETCH c_txn
              BULK COLLECT INTO l_vcb_list
              LIMIT  10000;

              FORALL i IN 1.. l_vcb_list.COUNT
                    UPDATE bk_account_history c SET  c.beneficiary_acct_no = l_vcb_list(i).beneficiary_acct_no,
                                                c.beneficiary_acct_name = l_vcb_list(i).beneficiary_acct_name,
                                                c.beneficiary_acct_bank = l_vcb_list(i).branch_name
                    WHERE ROWID =  l_vcb_list(i).a_rowid;

            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;
        END LOOP;
        CLOSE c_txn ;

        plog.setendsection( pkgctx, 'pr_benifit_vcb_in' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_vcb_in' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

 PROCEDURE pr_benifit_ibps_by_account(p_date7 NUMBER, p_account_no VARCHAR2)--#20161229 LocTX add
        IS
        CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE)
        IS
                SELECT  DISTINCT h.rowid  h_rowid,
                        (bm.bank_name) beneficiary_acct_bank, --#20160928 Loctx change from MIN
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.receive_account, msg.send_account)) beneficiary_acct_no,
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.recieve_name,msg.send_name))   beneficiary_acct_name
                FROM    bk_account_history h,
                        sync_cdc_ibps_msg_content msg,
                        sync_etl_ibps_bank_map bm,
                        sync_cdc_rmdetl rd
                WHERE 1 = 1
                AND h.rollout_acct_no = p_account_no
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                AND rd.rdjdat = p_cur_date7
                AND rd.rdauxt =  h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                AND rd.rdseq = h.tm_seq
                AND rd.rduser = TRIM(h.teller_id)--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD, #20181221 lOCTX BO SUNG TRIM 
                AND rd.rdrtyp <> 'G'
                AND rd.rdract = h.rollout_acct_no --#20150116 Loctx add for map giao dich  -- khong can AND rd.rdamt = h.amount vi co tmseq > 0
                --AND CAST (msg.rm_number AS NUMBER (18)) = rd.rdacct
                AND msg.rm_number = rd.rdacct--#20150527 Loctx change data type to number
                AND DECODE (msg.msg_direction,'SIBS-IBPS', msg.f19, msg.f21) = bm.gw_bank_code(+)
                AND  h.channel NOT IN
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
                --AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'--XU LY TUNG tk THI KHONG CAN
                --GROUP BY h.ROWID --#20160928 Loctx change from MIN
                ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_ibps' );

        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'));
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history
                SET beneficiary_acct_no     = l_txn_list(i).beneficiary_acct_no,
                    beneficiary_acct_name   = l_txn_list(i).beneficiary_acct_name,
                    beneficiary_acct_bank   = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;
        plog.setendsection( pkgctx, 'pr_benifit_ibps' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_ibps' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

/*
    Su dung trong truong hop bang rmdel theiu du lieu,
    p_account_no = NULL thi xu ly ALL tai khoan
*/
    PROCEDURE pr_benifit_ibps_out_pa2(p_date7 NUMBER, p_account_no VARCHAR2)
        IS
        CURSOR c_txn (p_cur_date_dt DATE, p_batch_no NUMBER, p_acct VARCHAR2)
        IS
                SELECT  DISTINCT h.row_id  h_rowid,
                        (bm.bank_name) beneficiary_acct_bank, --#20160928 Loctx change from MIN
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.receive_account, msg.send_account)) beneficiary_acct_no,
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.recieve_name,msg.send_name))   beneficiary_acct_name
                FROM    twtb_account_history_benifit h,
                        sync_cdc_ibps_msg_content msg,
                        sync_etl_ibps_bank_map bm--,
                        --sync_cdc_rmdetl rd
                WHERE 1 = 1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.batch_NO = p_batch_no
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                /*
                AND rd.rdjdat = p_cur_date7
                AND rd.rdauxt =  h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                AND rd.rdseq = h.tm_seq
                AND rd.rduser = h.teller_id--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
                AND rd.rdrtyp <> 'G'
                AND rd.rdract = h.rollout_acct_no --#20150116 Loctx add for map giao dich  -- khong can AND rd.rdamt = h.amount vi co tmseq > 0
                --AND CAST (msg.rm_number AS NUMBER (18)) = rd.rdacct
                AND msg.rm_number = rd.rdacct--#20150527 Loctx change data type to number
                AND DECODE (msg.msg_direction,'SIBS-IBPS', msg.f19, msg.f21) = bm.gw_bank_code(+)
                */

                AND msg.trans_Date >= p_cur_date_dt AND msg.trans_Date < p_cur_date_dt + 1

                AND h.amount = msg.amount
                AND h.remark LIKE '%(CKRmNo:%' || msg.rm_number || '%'--#20150527 Loctx change data type to number
                --AND DECODE (msg.msg_direction,'SIBS-IBPS', msg.f19, msg.f21) = bm.gw_bank_code(+)
                AND msg.msg_direction = 'SIBS-IBPS'
                AND msg.rm_number IS NOT NULL
                AND msg.f19 = bm.gw_bank_code(+)
                AND  h.channel NOT IN
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
                                        --/*
                AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
                    OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
                    OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$')
                    --*/
                AND (CASE WHEN p_acct IS NULL THEN h.rollout_acct_no ELSE p_acct END) = h.rollout_acct_no
                --GROUP BY h.ROWID --#20160928 Loctx change from MIN
                ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);
        l_batch_no NUMBER(20);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_ibps' );

        plog.debug( pkgctx, 'pr_benifit_ibps->Load txn begin' );

        SELECT TO_CHAR(SYSDATE, 'RRRRMMDD') || lpad(ibs.seq_batch_benifit_id.nextval, 10, '0')
        INTO l_batch_no
        FROM dual;

        pr_load_tw_txn(l_batch_no, TO_DATE(p_date7, 'RRRRDDD'));
--        dbms_output.put_line ('l_batch_no=' || l_batch_no);

        plog.debug( pkgctx, 'pr_benifit_ibps->Load txn end, then process' );

        OPEN c_txn(TO_DATE(p_date7, 'RRRRDDD'), l_batch_no, p_account_no);
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

        dbms_output.put_line ('l_txn_list.count=' || l_txn_list.COUNT);

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history
                SET beneficiary_acct_no     = l_txn_list(i).beneficiary_acct_no,
                    beneficiary_acct_name   = l_txn_list(i).beneficiary_acct_name,
                    beneficiary_acct_bank   = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;

        plog.debug( pkgctx, 'pr_benifit_ibps->process finished' );

        pr_unload_tw_txn(l_batch_no);

        plog.setendsection( pkgctx, 'pr_benifit_ibps' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            pr_unload_tw_txn(l_batch_no);

            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_ibps' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

/*
    Su dung trong truong hop bang rmdel theiu du lieu,
    p_account_no = NULL thi xu ly ALL tai khoan
*/
    PROCEDURE pr_benifit_ibps_in_pa2(p_date7 NUMBER, p_account_no VARCHAR2)
        IS
        CURSOR c_txn (p_cur_date_dt DATE, p_batch_no NUMBER, p_acct VARCHAR2)
        IS
                SELECT  DISTINCT h.row_id  h_rowid,
                        (bm.bank_name) beneficiary_acct_bank, --#20160928 Loctx change from MIN
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.receive_account, msg.send_account)) beneficiary_acct_no,
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.recieve_name,msg.send_name))   beneficiary_acct_name
                FROM    twtb_account_history_benifit h,
                        sync_cdc_ibps_msg_content msg,
                        sync_etl_ibps_bank_map bm--,
                        --sync_cdc_rmdetl rd
                WHERE 1 = 1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.batch_NO = p_batch_no
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                /*
                AND rd.rdjdat = p_cur_date7
                AND rd.rdauxt =  h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                AND rd.rdseq = h.tm_seq
                AND rd.rduser = h.teller_id--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
                AND rd.rdrtyp <> 'G'
                AND rd.rdract = h.rollout_acct_no --#20150116 Loctx add for map giao dich  -- khong can AND rd.rdamt = h.amount vi co tmseq > 0
                --AND CAST (msg.rm_number AS NUMBER (18)) = rd.rdacct
                AND msg.rm_number = rd.rdacct--#20150527 Loctx change data type to number
                AND DECODE (msg.msg_direction,'SIBS-IBPS', msg.f19, msg.f21) = bm.gw_bank_code(+)
                */

                AND msg.trans_Date >= p_cur_date_dt AND msg.trans_Date < p_cur_date_dt + 1

                AND h.amount = msg.amount
                --AND DECODE (msg.msg_direction,'SIBS-IBPS', msg.f19, msg.f21) = bm.gw_bank_code(+)
                AND msg.msg_direction = 'IBPS-SIBS'
                AND msg.rm_number IS NOT NULL
                AND msg.receive_account = h.rollout_acct_no
                AND msg.f21 = bm.gw_bank_code(+)
                AND  h.channel NOT IN
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
                                        --/*
                AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
                    OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
                    OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$')
                    --*/
                AND (CASE WHEN p_acct IS NULL THEN h.rollout_acct_no ELSE p_acct END) = h.rollout_acct_no
                --GROUP BY h.ROWID --#20160928 Loctx change from MIN
                ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);
        l_batch_no NUMBER(20);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_ibps' );

        plog.debug( pkgctx, 'pr_benifit_ibps->Load txn begin' );

        SELECT TO_CHAR(SYSDATE, 'RRRRMMDD') || lpad(ibs.seq_batch_benifit_id.nextval, 10, '0')
        INTO l_batch_no
        FROM dual;

        pr_load_tw_txn(l_batch_no, TO_DATE(p_date7, 'RRRRDDD'));
--        dbms_output.put_line ('l_batch_no=' || l_batch_no);

        plog.debug( pkgctx, 'pr_benifit_ibps->Load txn end, then process' );

        OPEN c_txn(TO_DATE(p_date7, 'RRRRDDD'), l_batch_no, p_account_no);
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

        dbms_output.put_line ('l_txn_list.count=' || l_txn_list.COUNT);

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history
                SET beneficiary_acct_no     = l_txn_list(i).beneficiary_acct_no,
                    beneficiary_acct_name   = l_txn_list(i).beneficiary_acct_name,
                    beneficiary_acct_bank   = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;

        plog.debug( pkgctx, 'pr_benifit_ibps->process finished' );

        pr_unload_tw_txn(l_batch_no);

        plog.setendsection( pkgctx, 'pr_benifit_ibps' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            pr_unload_tw_txn(l_batch_no);

            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_ibps' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

    PROCEDURE pr_benifit_ibps(p_date7 NUMBER)
        IS
        CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE, p_batch_no NUMBER)
        IS
                SELECT  DISTINCT h.row_id  h_rowid,
                        (bm.bank_name) beneficiary_acct_bank, --#20160928 Loctx change from MIN
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.receive_account, msg.send_account)) beneficiary_acct_no,
                        (DECODE (msg.msg_direction, 'SIBS-IBPS', msg.recieve_name,msg.send_name))   beneficiary_acct_name
                FROM    twtb_account_history_benifit h,
                        sync_cdc_ibps_msg_content msg,
                        sync_etl_ibps_bank_map bm,
                        sync_cdc_rmdetl rd
                WHERE 1 = 1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.batch_NO = p_batch_no
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                AND rd.rdjdat = p_cur_date7
                AND rd.rdauxt =  h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                AND rd.rdseq = h.tm_seq
                AND rd.rduser = TRIM(h.teller_id)--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD, #20181221 lOCTX BO SUNG TRIM 
                AND rd.rdrtyp <> 'G'
                AND rd.rdract = h.rollout_acct_no --#20150116 Loctx add for map giao dich  -- khong can AND rd.rdamt = h.amount vi co tmseq > 0
                --AND CAST (msg.rm_number AS NUMBER (18)) = rd.rdacct
                AND msg.rm_number = rd.rdacct--#20150527 Loctx change data type to number
                AND DECODE (msg.msg_direction,'SIBS-IBPS', msg.f19, msg.f21) = bm.gw_bank_code(+)
                AND  h.channel NOT IN
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
                AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
                    OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
                    OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$')
                --GROUP BY h.ROWID --#20160928 Loctx change from MIN
                ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);
        l_batch_no NUMBER(20);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_ibps' );

        plog.debug( pkgctx, 'pr_benifit_ibps->Load txn begin' );

        SELECT TO_CHAR(SYSDATE, 'RRRRMMDD') || lpad(ibs.seq_batch_benifit_id.nextval, 10, '0')
        INTO l_batch_no
        FROM dual;

        pr_load_tw_txn(l_batch_no, TO_DATE(p_date7, 'RRRRDDD'));
        plog.debug( pkgctx, 'pr_benifit_ibps->Load txn end, then process' );

        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'), l_batch_no);
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history
                SET beneficiary_acct_no     = l_txn_list(i).beneficiary_acct_no,
                    beneficiary_acct_name   = l_txn_list(i).beneficiary_acct_name,
                    beneficiary_acct_bank   = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;

        plog.debug( pkgctx, 'pr_benifit_ibps->process finished' );

        pr_unload_tw_txn(l_batch_no);

        plog.setendsection( pkgctx, 'pr_benifit_ibps' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            pr_unload_tw_txn(l_batch_no);

            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_ibps' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;
   -----------------------------------------------------------------------------

   PROCEDURE pr_benifit_swift_out(p_date7 NUMBER)
    IS

        CURSOR c_txn (p_cur_date NUMBER, p_cur_date_dt DATE)
        IS
                SELECT  DISTINCT h.rowid             h_rowid,
                        (msg.receive_account) beneficiary_acct_no, --#20160928 Loctx change from MIN
                        (bm.bank_name)        beneficiary_acct_bank,
                        (msg.receive_name)    beneficiary_acct_name
                FROM    bk_account_history          h,
                        sync_cdc_swift_msg_content  msg,
                        sync_etl_swift_bank_map     bm,
                        sync_cdc_rmdetl             rd
                WHERE 1 = 1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                AND rd.rdjdat = p_cur_date
                AND rd.rdauxt =  h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                AND rd.rdseq = h.tm_seq
                AND rd.rduser = TRIM(h.teller_id)--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD, #20181221 lOCTX BO SUNG TRIM 
                AND rd.rdrtyp <> 'G'
                AND rd.rdract = h.rollout_acct_no AND rd.rdamt = h.amount --#20150116 Loctx add for map giao dich
                --AND CAST (msg.rm_number AS NUMBER (18)) = rd.rdacct
                AND msg.rm_number = rd.rdacct--#20150527 Loctx chuyen rm_number thanh number
                AND msg.msg_direction = 'SIBS-SWIFT'
                --AND TRIM (msg.branch_b) = TRIM (bm.swift_bank_code(+)) --#20150527 Loctx change, ko can TRIM branch
                AND msg.branch_b = TRIM (bm.swift_bank_code(+)) --#20150527 Loctx change, ko can TRIM branch
                AND  h.channel NOT IN
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
                AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
                --GROUP BY h.rowid --#20160928 Loctx change from MIN
                ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_swipf_out' );

        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'));
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history
                SET beneficiary_acct_no     = l_txn_list(i).beneficiary_acct_no,
                    beneficiary_acct_name   = l_txn_list(i).beneficiary_acct_name,
                    beneficiary_acct_bank   = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;
        plog.setendsection( pkgctx, 'pr_benifit_swipf_out' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_swipf_out' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

    --------------------------------------------------------------------------

    PROCEDURE pr_benifit_swift_in(p_date7 NUMBER)
    IS

        CURSOR c_txn (p_cur_date NUMBER, p_cur_date_dt DATE)
        IS
                SELECT  DISTINCT h.rowid             h_rowid,
                        (msg.send_account)    beneficiary_acct_no, --#20160928 Loctx change from MIN
                        (msg.send_name)       beneficiary_acct_name,
                        (bm.bank_name)        beneficiary_acct_bank

                FROM    bk_account_history          h,
                        sync_cdc_swift_msg_content  msg,
                        sync_etl_swift_bank_map     bm,
                        sync_cdc_rmdetl             rd,
                        sync_cdc_rmmast             rm
                WHERE 1 = 1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt  AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                AND rd.rdjdat = p_cur_date
                AND NVL(trim(rd.rdauxt), '$X$') =  NVL(TRIM(h.trace_code), '$X$')
                AND rd.rdseq = h.tm_seq
                --#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
                AND trim(rd.rduser) = TRIM(h.teller_id)
                AND rd.rdrtyp <> 'G'
                AND rd.rdract = h.rollout_acct_no AND rd.rdamt = h.amount --#20150116 Loctx add for map giao dich
                AND TRIM(msg.f20) = TRIM(rm.rmref)
                --AND CAST (msg.rm_number AS NUMBER (18)) = rd.rdacct
                --AND msg.rm_number = rd.rdacct--#20150527 Loctx change rm number to number--#20150623 Loctx comment: chi swift out moi dung dieu kiennay
                AND msg.msg_direction = 'SWIFT-SIBS'
                AND rd.rdacct = rm.rmacno
                --AND TRIM (msg.branch_b) = TRIM (bm.swift_bank_code(+)) --#20150527 Loctx change, ko can TRIM branch
                AND msg.branch_b = TRIM (bm.swift_bank_code(+)) --#20150527 Loctx change, ko can TRIM branch
                AND  h.channel NOT IN
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
                AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
                --GROUP BY h.ROWID--#20160928 Loctx change from MIN
                ;


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_swipf_in' );

        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'));
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

            FORALL i IN 1..l_txn_list.count
                UPDATE bk_account_history
                SET beneficiary_acct_no     = l_txn_list(i).beneficiary_acct_no,
                    beneficiary_acct_name   = l_txn_list(i).beneficiary_acct_name,
                    beneficiary_acct_bank   = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;
        plog.setendsection( pkgctx, 'pr_benifit_swipf_in' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_swipf_in' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;


BEGIN
    SELECT   *
    INTO   logrow
    FROM   tlogdebug
    WHERE   ROWNUM <= 1;

    pkgctx      :=
    plog.init('cspkg_benifit_refresh_manual',
        plevel => logrow.loglevel,
        plogtable => ( logrow.log4table = 'Y' ),
        palert => ( logrow.log4alert = 'Y' ),
        ptrace => ( logrow.log4trace = 'Y' ) );
END;

/
