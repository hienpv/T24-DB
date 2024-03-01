--------------------------------------------------------
--  DDL for Package Body ACSPKG_BENIFIT_REFRESH_MANUAL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "ACSPKG_BENIFIT_REFRESH_MANUAL" 
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
        AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$');
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
                FROM bk_account_history h, sync_cdc_tmtran tm, cstb_account_info bka
                WHERE 1=1
                and h.rollout_acct_no = p_account_no
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1 --#20160723 Loctx change
                AND tm.tmentdt7 = p_cur_date7
                AND h.trace_code = tm.tmtxcd--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = tm.tmtxseq -- sequence
                AND h.teller_id = tm.tmtellid -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> tm.tmdorc -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND bka.acct_no(+) = tm.tmacctno
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
                AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$';


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
                FROM bk_account_history h, staging.si_his_ddhist@staging_pro ddhis, cstb_account_info bka
                WHERE 1=1
                and h.rollout_acct_no = p_account_no
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1 --#20160723 Loctx change
                AND ddhis.trdate = p_cur_date7
                AND h.trace_code = TRIM(ddhis.auxtrc)--tm.tmtxcd--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.tm_seq = ddhis.seq -- tm.tmtxseq -- sequence
                AND h.teller_id = TRIM(ddhis.truser)--tm.tmtellid -- ma giao dich vien --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND h.dc_sign <> TRIM (ddhis.dorc)--tm.tmdorc -- D and C --#20160928 Loctx da xu ly trim trong TMTRANCDC
                AND bka.acct_no(+) = ddhis.tracct--tm.tmacctno
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
                AND NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$';


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
                SELECT h.row_id h_rowid, tm.tmacctno beneficiary_acct_no,
                        'MARITIME BANK' beneficiary_acct_bank,
                        bka.acct_name beneficiary_acct_name
                FROM twtb_account_history_benifit h, sync_cdc_tmtran tm, cstb_account_info bka
                WHERE 1=1
                --AND TRUNC(h.post_time) = p_cur_date_dt
                AND h.batch_NO = p_batch_no
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1 --#20160723 Loctx change
                AND tm.tmentdt7 = p_cur_date7
                AND h.trace_code = tm.tmtxcd--channel txn - ma giao dich --#20160928 Loctx da xu ly trim trong TMTRANCDC
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
        AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$');


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
               AND rd.rduser = h.teller_id--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
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
                   AND rd.rduser = h.teller_id--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
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
                AND rd.rduser = h.teller_id--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
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


    PROCEDURE pr_benifit_ibps(p_date7 NUMBER)
        IS
        CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE)
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
                AND h.tran_time >= p_cur_date_dt AND h.tran_time < p_cur_date_dt + 1--#20160723 Loctx change
                AND rd.rdjdat = p_cur_date7
                AND rd.rdauxt =  h.trace_code--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                AND rd.rdseq = h.tm_seq
                AND rd.rduser = h.teller_id--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
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

        SELECT TO_CHAR(SYSDATE, 'RRRRMMDD') || lpad(ibs.seq_batch_benifit_id.nextval, 10, '0')
        INTO l_batch_no
        FROM dual;

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
        SELECT   l_batch_no,
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
        FROM ibs.bk_account_history h
        WHERE h.tran_time >= TO_DATE(p_date7, 'RRRRDDD') AND h.tran_time < TO_DATE(p_date7, 'RRRRDDD') + 1
        AND (NVL(h.beneficiary_acct_bank, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_name, '$NULL$') = '$NULL$'
        OR NVL(h.beneficiary_acct_no, '$NULL$') = '$NULL$');


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

        DELETE FROM twtb_account_history_benifit --#20170214 Loctx add
        WHERE batch_no = l_batch_no;

        COMMIT;
        plog.setendsection( pkgctx, 'pr_benifit_ibps' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            DELETE FROM twtb_account_history_benifit --#20170214 Loctx add
            WHERE batch_no = l_batch_no;

            COMMIT;

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
                AND rd.rduser = h.teller_id--#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
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
                AND NVL(rd.rdauxt, '$X$') =  NVL(TRIM(h.trace_code), '$X$')--#20160928 Loctx da xu ly trim (rd.rdauxt) trong TMTRANCD
                AND rd.rdseq = h.tm_seq
                --#20160928 Loctx da xu ly trim (rd.rduser) trong TMTRANCD
                AND rd.rduser = h.teller_id-- #20150623 Loctx for fix loi khong tim thay thu huong swift in
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
