--------------------------------------------------------
--  DDL for Package Body CSPKG_TRANSACTION_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_TRANSACTION_SYNC" 
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
     **  HaoNS      09-SEP-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
----------------------------------------------------------------------------------------------------*/

 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_salary_onday_sync( dm_operation_type in CHAR,
                                    orchestrate_trdate in NUMBER,
                                    orchestrate_treffd in NUMBER,
                                    orchestrate_trtime in NUMBER,
                                    orchestrate_seq in VARCHAR,
                                    orchestrate_truser in VARCHAR,
                                    orchestrate_dorc in VARCHAR,
                                    orchestrate_amt in NUMBER,
                                    orchestrate_trctyp in VARCHAR,
                                    orchestrate_trefth in VARCHAR2,
                                    orchestrate_tracct in NUMBER,
                                    orchestrate_trancd in NUMBER
                                    )
    IS
        p_min_value       NUMBER;
        l_val             NUMBER;
        l_check NUMBER;
        l_acc_no VARCHAR2(50);
        l_err_desc VARCHAR2(4000);
    BEGIN
        IF dm_operation_type = 'I' AND orchestrate_dorc IN ('D','C') THEN
            l_acc_no := (case when length(orchestrate_tracct) = 13 then LPAD (orchestrate_tracct, 14, '0') else TO_CHAR(orchestrate_tracct) end);

            SELECT CASE WHEN EXISTS(SELECT 1 FROM bk_account_info a,
                                (
                                          SELECT cif_no
                                          FROM   bb_corp_info
                                  )b
                                  WHERE a.cif_no = b.cif_no
                                  AND a.acct_no = l_acc_no)
                        THEN 1
                        ELSE 0
                    END
            INTO l_check
            FROM dual;

            IF l_check = 1 THEN

                INSERT INTO bk_account_history (core_sn,
                                                post_time,
                                                tran_time,
                                                teller_id, --#202141230 Loctx add
                                                tm_seq, --#202141230 Loctx add
                                                dc_sign,
                                                amount,
                                                currency_code,
                                                pre_balance,
                                                channel,
                                                remark,
                                                rollout_acct_no,
                                                insert_date,
                                                status,
                                                sync_type,
                                                tran_service_code)
                    (SELECT
                           'SL'
                              || TO_CHAR (orchestrate_trdate)
                              || seq_core_sn_salary.NEXTVAL,
                              TO_DATE(orchestrate_trdate || ':' || LPAD(orchestrate_trtime, 6, '0'), 'yyyyddd:hh24miss'),
                              DECODE (LENGTH (orchestrate_treffd),
                                      7, TO_DATE (orchestrate_treffd || ':' || LPAD(orchestrate_trtime, 6, '0'), 'yyyyddd:hh24miss'),
                                      TO_DATE(orchestrate_trdate || ':' || LPAD(orchestrate_trtime, 6, '0'), 'yyyyddd:hh24miss')), --20160226 QuanDH thay doi, neu trantime NULL thi lay POSTTIME: fix van de trung giao dich luong
                              TRIM(orchestrate_truser),
                              orchestrate_seq,
                              TRIM (orchestrate_dorc),
                              orchestrate_amt,
                              TRIM (orchestrate_trctyp),
                              NULL,
                              'CNT',

                              SUBSTR (orchestrate_trefth, 11, LENGTH (orchestrate_trefth))
                                  AS trefth,
                              (case when length(orchestrate_tracct) = 13 then LPAD (orchestrate_tracct, 14, '0') else TO_CHAR(orchestrate_tracct) end),
                              SYSDATE,
                              'SUCC',
                              '5',                                    --salary
                              DECODE (d.tran_service_code,
                                      NULL, 'RTR001',
                                      'RTR002')
                       FROM DUAL
                       LEFT JOIN sync_tran_code d ON d.tran_service_code = TO_CHAR(orchestrate_trancd)
                        );

                COMMIT;
            END IF;

        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'DDTRN2', 'BK_ACCOUNT_HISTORY',
                                                    orchestrate_tracct, dm_operation_type, l_err_desc);

            COMMIT;
            RAISE;
    END;


 /*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/

  ------------HAONS CHANGE 20150410
    PROCEDURE  pr_tmtran_fail_later_process
    IS
        l_status VARCHAR2(20);
        l_tran_service_code VARCHAR2(20);
        l_channel VARCHAR2(3);
        l_remark VARCHAR2(500);
        l_fail_rowid VARCHAR2(100);
        l_fail_back_rowid VARCHAR2(100);
        l_err_desc VARCHAR2(256);
        l_taget_table VARCHAR2(30);

        CURSOR cursor_account_hist IS
                    SELECT a.status,
                            a.tran_service_code,
                            a.channel,
                            a.remark,
                            c.rowid hist_rowid,
                            a.rowid fail_rowid
                    FROM bk_account_history c,
                        sync_cdc_bk_acct_hist_fail a
                    WHERE a.teller_id = c.teller_id
                        AND a.post_time = c.post_time
                        AND a.dc_sign = c.dc_sign
                        AND a.tm_seq = c.tm_seq
                        AND a.rollout_acct_no = c.rollout_acct_no
                        AND a.amount = c.amount
                        AND a.tran_device = c.tran_device
                        AND a.device_no = c.device_no
                        AND a.process_ind = 'N'
                        AND c.status = 'SUCC';


        TYPE ty_txn_fail_tb IS TABLE OF cursor_account_hist%ROWTYPE INDEX BY PLS_INTEGER;

        l_txn_list ty_txn_fail_tb;
        l_txn cursor_account_hist%ROWTYPE;

    BEGIN
        OPEN cursor_account_hist;
        LOOP
            FETCH cursor_account_hist
            BULK COLLECT INTO l_txn_list LIMIT 1000;

            FOR idx IN 1..l_txn_list.COUNT LOOP
                l_txn :=  l_txn_list(idx);

                l_taget_table := 'BK_ACCOUNT_HISTORY';
                UPDATE bk_account_history c
                    SET c.status = l_txn.status,
                        c.tran_service_code = l_txn.tran_service_code,
                        c.channel = l_txn.channel,
                        c.remark = l_txn.remark
                WHERE ROWID = l_txn.hist_rowid;

                l_taget_table := 'sync_cdc_bk_acct_hist_fail';
                UPDATE sync_cdc_bk_acct_hist_fail
                        SET process_ind = 'Y'
                WHERE ROWID =  l_txn.fail_rowid;
            END LOOP;

            COMMIT;

            EXIT WHEN cursor_account_hist%NOTFOUND;

        END LOOP;

        CLOSE cursor_account_hist;
        COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            CLOSE cursor_account_hist;

            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN_BACK_FAIL', l_taget_table,
                                                    'xxxx', 'U', l_err_desc);

            COMMIT;
            RAISE;
 END;
 PROCEDURE  pr_tmtran_fail_sync (dm_operation_type in CHAR,
                               orchestrate_tmtxcd in VARCHAR,
                                orchestrate_tmresv07 in VARCHAR,
                                orchestrate_tmdorc in CHAR,
                                orchestrate_tmtxamt in NUMBER,
                                orchestrate_tmglcur in VARCHAR,
                                orchestrate_tmorgamt in NUMBER,
                                orchestrate_tmefth in VARCHAR2,
                                orchestrate_tmacctno in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                orchestrate_tmtellid in VARCHAR,
                                orchestrate_tmtxseq in VARCHAR,  -- quandh3 2023 T24 change type number to varchar
                                orchestrate_tmtxstat in VARCHAR,
                                orchestrate_tmhosttxcd in NUMBER,
                                orchestrate_tmapptype in CHAR,
                                orchestrate_tmeqvtrn in CHAR,
                                orchestrate_tmibttrn in CHAR,
                                orchestrate_tmsumtrn in CHAR,
                                orchestrate_tmentdt7 in NUMBER,
                                orchestrate_tmeffdt7 in NUMBER,
                                orchestrate_tmsseq in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                orchestrate_tmtiment in NUMBER)
    IS
        l_check_exists NUMBER := 0;
        l_check_exists_account NUMBER := 0;
        l_check_exists_insert NUMBER := 0;
        l_acct_no VARCHAR2(20);
        l_err_desc VARCHAR2(2000);
        l_taget_table VARCHAR2(100);
        l_tran_sn sync_cdc_tranmap.tran_sn%type;
        l_sender_id sync_cdc_tranmap.sender_id%type;
        l_tran_service_code sync_tran_code.tran_service_code%type;
        l_rowid ROWID;
    BEGIN
    
    
CSPKS_CDC_T24_UTIL.PR_T24_CDD_LOG_TMTRAN_FAIL (  dm_operation_type ,  orchestrate_tmtxcd   , orchestrate_tmresv07 ,  orchestrate_tmdorc   ,
                                  orchestrate_tmtxamt  , orchestrate_tmglcur  ,  orchestrate_tmorgamt , orchestrate_tmefth   , 
                                  orchestrate_tmacctno ,  orchestrate_tmtellid ,  orchestrate_tmtxseq  ,  orchestrate_tmtxstat , 
                                  orchestrate_tmhosttxcd , orchestrate_tmapptype ,  orchestrate_tmeqvtrn ,   orchestrate_tmibttrn , 
                                  orchestrate_tmsumtrn ,  orchestrate_tmentdt7 ,  orchestrate_tmeffdt7 ,   orchestrate_tmsseq   ,
                                  orchestrate_tmtiment  , 0 , 0 , 'CDD INPUT' ) ;
                                  
        IF dm_operation_type <> 'D'--#20150529 LocTx change from U
            AND TRIM(orchestrate_tmtxstat) = 'CE'
            AND orchestrate_tmapptype <> 'G'
            AND RTRIM(orchestrate_tmresv07) IS NOT NULL
            AND (orchestrate_tmsumtrn IS NULL OR orchestrate_tmsumtrn <> 'N')
            AND (orchestrate_tmibttrn IS NULL OR orchestrate_tmibttrn <> 'Y')
            AND (orchestrate_tmeqvtrn IS NULL OR orchestrate_tmeqvtrn <> 'I')
            AND orchestrate_tmhosttxcd NOT IN (77,
                                      129,
                                      178,
                                      179,
                                      185)
            AND    orchestrate_tmapptype IS NOT NULL
            AND    orchestrate_tmdorc IN ('D', 'C')
        THEN
             l_acct_no := (case when length(orchestrate_tmacctno) = 13 then LPAD (orchestrate_tmacctno, 14, '0') else TO_CHAR(orchestrate_tmacctno) end);

            SELECT CASE
                    WHEN  EXISTS ( SELECT 1
                                    FROM bk_account_info a,
                                    (
                                        SELECT cif_no
                                        FROM   bb_corp_info
                                    ) b
                                   WHERE a.cif_no = b.cif_no
                                   --WHERE 1=1
                                   AND a.acct_no = l_acct_no--#20150327 Loctx change from trim(a.acct_no)
                                ) THEN 1
                        ELSE 0
                        END
            INTO  l_check_exists
            FROM DUAL;

            IF l_check_exists = 1 THEN

                BEGIN
                    SELECT MAX(tran_sn), MAX(sender_id)
                    INTO l_tran_sn, l_sender_id
                    FROM  sync_cdc_tranmap b
                    WHERE  b.teller_id = TRIM(orchestrate_tmtellid)
                    AND    b.host_tran_sn = orchestrate_tmtxseq--TO_CHAR(orchestrate_tmtxseq)----#20150527 Loctx tuniing, chang to number
                    AND    b.host_real_date = orchestrate_tmentdt7--TO_CHAR(orchestrate_tmentdt7)----#20150527 Loctx tuniing, chang to number
                    AND b.sorn = 'Y'; --#20150526 LocTX add

                EXCEPTION--#20150425 Loctx add check exception
                    WHEN NO_DATA_FOUND THEN
                        NULL;
                END;

                l_taget_table := 'BK_ACCOUNT_HISTORY';

                UPDATE bk_account_history c
                    SET    c.status = DECODE(TRIM(orchestrate_tmtxstat),
                                                          'CE',
                                                          'FAIL',
                                                          'SUCC'),
                             c.tran_service_code =
                             (SELECT DECODE(d.tran_service_code,
                                              NULL,
                                              'RTR001',
                                          'RTR002')
                                    FROM sync_tran_code d
                                    WHERE d.tran_code = TRIM(orchestrate_tmtxcd)
                                ),
                             c.channel = DECODE(l_sender_id,
                                                NULL,
                                                'CNT',
                                                l_sender_id),
                             c.remark  = orchestrate_tmefth
                WHERE c.teller_id = TRIM(orchestrate_tmtellid)
                        AND c.tm_seq = orchestrate_tmtxseq
                        AND TRUNC(c.post_time) = TO_DATE(orchestrate_tmentdt7,'YYYYDDD')
                        AND c.rollout_acct_no = l_acct_no
                        AND c.dc_sign = TRIM(orchestrate_tmdorc)
                        AND c.tran_device = orchestrate_tmsseq
                        AND c.device_no = TRIM(orchestrate_tmhosttxcd)
                RETURNING MAX(ROWID) INTO l_rowid;

                --LUU LAI DE XU LY SAU
                IF l_rowid IS NULL THEN
                    INSERT INTO sync_cdc_bk_acct_hist_fail c(c.core_sn,
                                                         c.tran_time,
                                                         c.post_time,
                                                         c.dc_sign,
                                                         c.amount,
                                                         c.currency_code,
                                                         c.pre_balance,
                                                         c.channel,
                                                         c.remark,
                                                         c.rollout_acct_no,
                                                         c.insert_date,
                                                         c.teller_id,
                                                         c.tm_seq,
                                                         c.sync_type,
                                                         c.status,
                                                         c.tc_code,
                                                         c.tran_sn,
                                                         c.tran_service_code,
                                                         c.tran_device,
                                                         c.device_no,
                                                         c.trace_code,
                                                         c.process_ind )
                        (SELECT TRIM(orchestrate_tmresv07) || seq_core_sn.NEXTVAL,
                                                        TO_DATE((orchestrate_tmeffdt7 || ':' ||
                                                                LPAD(orchestrate_tmtiment,
                                                                      6,
                                                                      '0')),
                                                                'yyyyddd:hh24miss') AS tran_date,
                                                        TO_DATE((orchestrate_tmentdt7 || ':' ||
                                                                LPAD(orchestrate_tmtiment,
                                                                      6,
                                                                      '0')),
                                                                'yyyyddd:hh24miss') AS post_date,
                                                        TRIM(orchestrate_tmdorc) AS tmdorc,
                                                        orchestrate_tmtxamt AS tmtxamt,
                                                        TRIM(orchestrate_tmglcur) AS tmglcur,
                                                        orchestrate_tmorgamt AS tmorgamt,
                                                            DECODE(l_sender_id,
                                                            NULL,
                                                            'CNT',
                                                           l_sender_id)AS sender_id,
                                                        SUBSTR(orchestrate_tmefth,
                                                               11,
                                                               LENGTH(orchestrate_tmefth)) AS tmefth,
                                                        l_acct_no AS tmacctno,
                                                        SYSDATE AS tminsdate,
                                                        TRIM(orchestrate_tmtellid) AS tmtellid,
                                                        orchestrate_tmtxseq AS tmtxseq,
                                                         '0',
                                                         DECODE(TRIM(orchestrate_tmtxstat),
                                                            'CE',
                                                            'FAIL',
                                                            'SUCC')AS tmtxstat,
                                                         'SYNC',
                                                        l_tran_sn AS tran_sn,
                                                         (SELECT DECODE(d.tran_service_code,
                                                                                          NULL,
                                                                                          'RTR001',
                                                                                      'RTR002')
                                                                                FROM sync_tran_code d
                                                                                WHERE d.tran_code = TRIM(orchestrate_tmtxcd)
                                                                            ) AS tran_service_code,
                                                        orchestrate_tmsseq AS tmsseq,
                                                        orchestrate_tmhosttxcd AS tmhosttxcd,
                                                        orchestrate_tmtxcd AS tmtxcd,
                                                        'N' --chua xu ly
                                                        FROM dual
                                                        );
                END IF;

                l_taget_table := 'BB_TRANSFER_HISTORY';
                UPDATE bb_transfer_history c
                SET     c.status  = DECODE(TRIM(orchestrate_tmtxstat),
                                'CE',
                                'FAIL',
                                'SUCC'),
                        c.core_sn = orchestrate_tmtxseq
                WHERE c.tran_sn = l_tran_sn
                    AND c.rollout_acct_no = l_acct_no
                    AND c.amount = orchestrate_tmtxamt;

--                l_taget_table := 'BC_TRANSFER_HISTORY';

--                UPDATE bc_transfer_history c
--                SET     c.status  = DECODE(TRIM(orchestrate_tmtxstat),
--                                    'CE',
--                                    'FAIL',
--                                    'SUCC'),
--                        c.core_sn = orchestrate_tmtxseq
--                WHERE c.tran_sn = l_tran_sn
--                    AND c.rollout_account_no = l_acct_no
--                    AND c.amount = orchestrate_tmtxamt;

             /*  cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN xxx', 'TMTRANxxx',
                                orchestrate_tmacctno, dm_operation_type,
                                'orchestrate_tmentdt7= '|| orchestrate_tmentdt7
                                || ', orchestrate_tmtellid=' || orchestrate_tmtellid
                                || ', orchestrate_tmtxseq=' || orchestrate_tmtxseq
                                  || ', orchestrate_tmtxstat=' || orchestrate_tmtxstat
                                    ||',l_rowid='||l_rowid
                                  || ', orchestrate_tmefth=' || SUBSTR(orchestrate_tmefth,0,100)
                                );*/

                COMMIT;
            END IF;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1,200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN_FAIL', l_taget_table,
                                                    orchestrate_tmacctno, dm_operation_type, l_err_desc);

CSPKS_CDC_T24_UTIL.PR_T24_CDD_LOG_TMTRAN_FAIL (  dm_operation_type ,  orchestrate_tmtxcd   , orchestrate_tmresv07 ,  orchestrate_tmdorc   ,
                                  orchestrate_tmtxamt  , orchestrate_tmglcur  ,  orchestrate_tmorgamt , orchestrate_tmefth   , 
                                  orchestrate_tmacctno ,  orchestrate_tmtellid ,  orchestrate_tmtxseq  ,  orchestrate_tmtxstat , 
                                  orchestrate_tmhosttxcd , orchestrate_tmapptype ,  orchestrate_tmeqvtrn ,   orchestrate_tmibttrn , 
                                  orchestrate_tmsumtrn ,  orchestrate_tmentdt7 ,  orchestrate_tmeffdt7 ,   orchestrate_tmsseq   ,
                                  orchestrate_tmtiment  , 0 ,-1 , l_err_desc ) ;
            COMMIT;
            RAISE;
    END;

    PROCEDURE  pr_tmtran_sync (dm_operation_type in CHAR,
                                orchestrate_tmtxcd in VARCHAR,
                                orchestrate_tmresv07 in VARCHAR,
                                orchestrate_tmdorc in CHAR,
                                orchestrate_tmtxamt in NUMBER,
                                orchestrate_tmglcur in VARCHAR,
                                orchestrate_tmorgamt in NUMBER,
                                orchestrate_tmefth in VARCHAR2,
                                orchestrate_tmacctno in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                orchestrate_tmtellid in VARCHAR,
                                orchestrate_tmtxseq  in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                orchestrate_tmtxstat in VARCHAR,
                                orchestrate_tmhosttxcd in NUMBER,
                                orchestrate_tmapptype in CHAR,
                                orchestrate_tmeqvtrn in CHAR,
                                orchestrate_tmibttrn in CHAR,
                                orchestrate_tmsumtrn in CHAR,
                                orchestrate_tmentdt7 in NUMBER,
                                orchestrate_tmeffdt7 in NUMBER,
                                orchestrate_tmsseq in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                orchestrate_tmtiment in NUMBER)
    IS
        l_check_exists NUMBER := 0;
        l_acct_no VARCHAR2(20);
        l_err_desc VARCHAR2(250);
        l_tmtran_fail CHAR(1) := 'N';
        l_taget_table VARCHAR2(100);

        l_tran_sn bk_account_history.tran_sn%type;
        l_sender_id VARCHAR2(50);
        l_status VARCHAR2(10);
        l_acct  NUMBER := 0;
        CURSOR l_history  IS
        SELECT
                             (TRIM(orchestrate_tmresv07) || seq_core_sn.NEXTVAL) core_sn,
                                TO_DATE((orchestrate_tmeffdt7 || ':' ||
                                        LPAD(orchestrate_tmtiment,
                                              6,
                                              '0')),
                                        'yyyyddd:hh24miss') AS tran_date,
                                TO_DATE((orchestrate_tmentdt7 || ':' ||
                                        LPAD(orchestrate_tmtiment,
                                              6,
                                              '0')),
                                        'yyyyddd:hh24miss') AS post_date,
                                TRIM(orchestrate_tmdorc) AS tmdorc,
                                orchestrate_tmtxamt AS tmtxamt,
                                TRIM(orchestrate_tmglcur) AS tmglcur,
                                orchestrate_tmorgamt AS tmorgamt,
                                --#20151001 LocTx change from --DECODE(l_sender_id, NULL, 'CNT', l_sender_id) AS sender_id,
                                (CASE WHEN orchestrate_tmtellid LIKE '%EBANK%' THEN 'IBS' ELSE 'CNT' END) AS sender_id,
                                --SUBSTR(orchestrate_tmefth, 11, LENGTH(orchestrate_tmefth)) AS tmefth,
                                -- nang cap thay doi noi dung cho trancd 160
                                DECODE(orchestrate_tmhosttxcd,'160','Tra lai TK KKH', SUBSTR(orchestrate_tmefth, 11, LENGTH(orchestrate_tmefth))) AS tmefth,
                                l_acct_no AS tmacctno,
                                SYSDATE insert_date,
                                TRIM(orchestrate_tmtellid) AS tmtellid,
                                orchestrate_tmtxseq AS tmtxseq,
                                (CASE
                                    WHEN l_tmtran_fail= 'Y' THEN '0'
                                 ELSE
                                     DECODE(TRIM(orchestrate_tmapptype),
                                            'D',
                                            '6',
                                            'T',
                                            '7',
                                            'L',
                                            '8',
                                            'S',
                                            '6',
                                            '0')
                                 END)   sync_type,
                                l_status AS tmtxstat,
                                'SYNC' tc_code,
                                l_tran_sn AS tran_sn,
                                fn_get_tran_service_code(tran_service_code) AS tran_service_code,
                                orchestrate_tmsseq AS tmsseq,
                                orchestrate_tmhosttxcd AS tmhosttxcd,
                                TRIM(orchestrate_tmtxcd) AS tmtxcd
                    FROM DUAL
                    LEFT JOIN sync_tran_code d ON TRIM(orchestrate_tmtxcd) = d.tran_code;
    c_history l_history%ROWTYPE;
    BEGIN
    dbms_output.put_line('start');
 cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'BEGIN pr_tmtran_sync' );  
                                     
CSPKS_CDC_T24_UTIL.PR_T24_CDD_LOG_TMTRAN ( dm_operation_type , orchestrate_tmtxcd   ,   orchestrate_tmresv07 ,   orchestrate_tmdorc   ,
                                  orchestrate_tmtxamt  , orchestrate_tmglcur  , orchestrate_tmorgamt , orchestrate_tmefth   ,   orchestrate_tmacctno , 
                                  orchestrate_tmtellid ,  orchestrate_tmtxseq  ,  orchestrate_tmtxstat ,  orchestrate_tmhosttxcd ,  orchestrate_tmapptype  ,
                                  orchestrate_tmeqvtrn   , orchestrate_tmibttrn   ,  orchestrate_tmsumtrn   , orchestrate_tmentdt7   ,     orchestrate_tmeffdt7   ,
                                  orchestrate_tmsseq     , orchestrate_tmtiment   , 0 , 0 , 'CDD INPUT' ) ;
                                                     /*****************************************/
                   /* Quandh3 : Tam xu ly bý pass seq kys tu Fxxxx   */
                  
                    -- IF ( NVL(LENGTH(TRIM(TRANSLATE(orchestrate_tmtxseq, ' +-.0123456789', ' '))), 0) > 0) -- bo qua orchestrate_tmtxseq ko phai la so
                    --    THEN                 RETURN ;
                    --END IF;
                       
                    /* Quandh3 : Tam xu ly pass seq kys tu Fxxxx   */
                   /*****************************************/  
        dbms_output.put_line('------------------------1');           
        IF dm_operation_type = 'I'
            AND orchestrate_tmapptype <> 'G'
            AND RTRIM(orchestrate_tmresv07) IS NOT NULL
            AND (orchestrate_tmsumtrn IS NULL OR orchestrate_tmsumtrn <> 'N')
            AND (orchestrate_tmibttrn IS NULL OR orchestrate_tmibttrn <> 'Y')
            AND (orchestrate_tmeqvtrn IS NULL OR orchestrate_tmeqvtrn <> 'I')
            AND orchestrate_tmhosttxcd NOT IN (77,
                                      129,
                                      178,
                                      185)
            AND    orchestrate_tmapptype IS NOT NULL
            AND    orchestrate_tmdorc IN ('D', 'C')
        THEN
            -- l_check_exists := 1;
            dbms_output.put_line('------------------------2');     
       cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'pr_tmtran_sync : dm_operation_type = I...' );  
            l_acct_no := (case when length(orchestrate_tmacctno) = 13 then LPAD (orchestrate_tmacctno, 14, '0') else TO_CHAR(orchestrate_tmacctno) end);


            -- Neu giao dich duoc thuc hien boi Ebank thi khong can check CIF co su dung Ebank hay khong
            IF orchestrate_tmtellid like '%EBANKING%'
                OR TRIM(orchestrate_tmapptype) = 'T' --#20150508 LOctx add: TK CD khong check, xu ly truong hop cdtnew den sau tmtran
            THEN
                l_check_exists := 1;
                
                   cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'pr_tmtran_sync : l_check_exists = 1' );  
                                                     
            ELSE
                SELECT CASE
                    WHEN  EXISTS ( SELECT 1
                                    FROM bk_account_info a,
                                    (
                                        SELECT cif_no
                                        FROM   bb_corp_info
                                    ) b
                                   WHERE a.cif_no = b.cif_no
                                   --WHERE 1=1
                                   AND a.acct_no = to_char(l_acct_no)--#20150327 Loctx change from trim(a.acct_no)
                                ) THEN 1
                        ELSE 0
                        END
                INTO  l_check_exists
                FROM DUAL;
                  cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'pr_tmtran_sync : l_check_exists == '||l_check_exists );  
            END IF;
dbms_output.put_line('------------------------4'); 
            SELECT  DECODE(TRIM(orchestrate_tmtxstat),
                                                'CE',
                                                'FAIL',
                                                'SUCC')
            INTO l_status
            FROM DUAL;
dbms_output.put_line('-----------------------5'); 
            IF(orchestrate_tmtxstat = 'CE' --giao dich huy --#20141026 Loctx add: xu ly job TMTRAN_FAIL
                    AND orchestrate_tmhosttxcd NOT IN (179)
            )
            THEN
                l_tmtran_fail := 'Y';
            END IF;
dbms_output.put_line('------------------------6'); 
            IF l_check_exists = 1 AND l_tmtran_fail = 'N' THEN --haons add 20150410
                --#20150804 Loctx add
                BEGIN
                dbms_output.put_line('------------------------7'); 
                    SELECT MAX(tran_sn), MAX(sender_id)
                    INTO l_tran_sn, l_sender_id
                    FROM  sync_cdc_tranmap b
                    WHERE  b.teller_id = TRIM(orchestrate_tmtellid)
                    AND    b.host_tran_sn = orchestrate_tmtxseq --TO_CHAR(orchestrate_tmtxseq)--#20150527 Loctx tuniing, chang to number
                    AND    b.host_real_date = orchestrate_tmentdt7--TO_CHAR(orchestrate_tmentdt7)--#20150527 Loctx tuniing, chang to number
                    AND b.sorn = 'Y'; --#20150526 LocTX add
  cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'SELECT MAX(tran_sn), MAX(sender_id) '  ); 
                EXCEPTION--#20150425 Loctx add check exception
                    WHEN NO_DATA_FOUND THEN
                       cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'pr_tmtran_sync : NO_DATA_FOUND '  ); 
                        NULL;
                END;
                dbms_output.put_line('------------------------8');
                    l_taget_table := 'BK_ACCOUNT_HISTORY';
                     cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', l_taget_table,
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     ' INSERT pr_tmtran_sync' ); 
                                                     dbms_output.put_line('------------------------3');
               OPEN l_history;
                LOOP
                FETCH  l_history  INTO c_history;
                        EXIT WHEN l_history%notfound;
                        INSERT INTO bk_account_history c
                            (c.core_sn,
                             c.tran_time,
                             c.post_time,
                             c.dc_sign,
                             c.amount,
                             c.currency_code,
                             c.pre_balance,
                             c.channel,
                             c.remark,
                             c.rollout_acct_no,
                             c.insert_date,
                             c.teller_id,
                             c.tm_seq,
                             c.sync_type,
                             c.status,
                             c.tc_code,
                             c.tran_sn,
                             c.tran_service_code,
                             c.tran_device,
                             c.device_no,
                             c.trace_code)
                             VALUES (
                             c_history.core_sn,
                             c_history.tran_date,
                             c_history.post_date,
                             c_history.tmdorc,
                             c_history.tmtxamt,
                             c_history.tmglcur,
                             c_history.tmorgamt,
                             c_history.sender_id,
                             c_history.tmefth,
                             c_history.tmacctno,
                             c_history.insert_date,
                             c_history.tmtellid,
                             c_history.tmtxseq,
                             c_history.sync_type,
                             c_history.tmtxstat,
                             c_history.tc_code,
                             c_history.tran_sn,
                             c_history.tran_service_code,
                             c_history.tmsseq,
                             c_history.tmhosttxcd,
                             c_history.tmtxcd
                             );
                    END LOOP;
                          /*
                          -- commend tmtr24
                            where  not exists (select * from bk_account_history_temp tmp
                              where  tmp.teller_id = trim(orchestrate_tmtellid) AND   tmp.tm_seq = orchestrate_tmtxseq
                              AND   tmp.AMOUNT = orchestrate_tmtxamt
                              AND  tmp.tran_time = orchestrate_tmeffdt7   )
                              */
                    
                IF l_tran_sn IS NOT NULL THEN
--                    l_taget_table := 'BC_TRANSFER_HISTORY';
--                    UPDATE bc_transfer_history c SET
--                          c.status  = l_status,
--                             c.core_sn = orchestrate_tmtxseq
--                    WHERE c.tran_sn = l_tran_sn
--                    AND c.rollout_account_no = l_acct_no
--                    AND c.amount = orchestrate_tmtxamt;

                    l_taget_table := 'BB_TRANSFER_HISTORY';
                     cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', l_taget_table,
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     ' UPDATE pr_tmtran_sync' ); 
                    UPDATE BB_TRANSFER_HISTORY c SET
                          c.status  = l_status,
                             c.core_sn = orchestrate_tmtxseq
                    WHERE c.tran_sn = l_tran_sn
                    AND c.rollout_acct_no = l_acct_no
                    AND c.amount = orchestrate_tmtxamt;


                END IF;
                cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN',  '',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     ' END pr_tmtran_sync' );
                COMMIT;
            END IF;
        END IF;
            
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', l_taget_table,
                                                    orchestrate_tmacctno, dm_operation_type, l_err_desc);
CSPKS_CDC_T24_UTIL.PR_T24_CDD_LOG_TMTRAN ( dm_operation_type , orchestrate_tmtxcd   ,   orchestrate_tmresv07 ,   orchestrate_tmdorc   ,
                                  orchestrate_tmtxamt  , orchestrate_tmglcur  , orchestrate_tmorgamt , orchestrate_tmefth   ,   orchestrate_tmacctno , 
                                  orchestrate_tmtellid ,  orchestrate_tmtxseq  ,  orchestrate_tmtxstat ,  orchestrate_tmhosttxcd ,  orchestrate_tmapptype  ,
                                  orchestrate_tmeqvtrn   , orchestrate_tmibttrn   ,  orchestrate_tmsumtrn   , orchestrate_tmentdt7   ,     orchestrate_tmeffdt7   ,
                                  orchestrate_tmsseq     , orchestrate_tmtiment   , 0 , -1 , l_err_desc ) ;
            COMMIT;
            RAISE;
    END;

/* dung CDC truc tiep
PROCEDURE pr_tranmap_sync (dm_operation_type IN CHAR,
                            orchestrate_teller_id IN VARCHAR,
                            orchestrate_host_tran_sn  IN VARCHAR,
                            orchestrate_host_real_date IN VARCHAR,
                            orchestrate_sender_id  IN VARCHAR,
                            orchestrate_message_date IN VARCHAR,
                            orchestrate_tran_sn IN VARCHAR,
                            orchestrate_sorn IN CHAR )
    IS
        l_err_desc VARCHAR2(250);
        l_check NUMBER;
    BEGIN
            IF dm_operation_type = 'I' THEN
                SELECT CASE WHEN EXISTS(SELECT 1
                                            FROM sync_cdc_tranmap
                                                WHERE host_tran_sn = orchestrate_host_tran_sn )
                            THEN 1
                            ELSE 0
                       END
                       INTO l_check
                       FROM DUAL;

            IF l_check = 0 THEN
               INSERT INTO sync_cdc_tranmap c
                            (c.tran_sn,
                            c.teller_id,
                            c.host_tran_sn,
                            c.host_real_date,
                            c.sender_id,
                            c.message_date,
                            c.sorn)
                     VALUES (orchestrate_tran_sn,
                        orchestrate_teller_id,
                        orchestrate_host_tran_sn,
                        orchestrate_host_real_date,
                        orchestrate_sender_id,
                        orchestrate_message_date,
                        orchestrate_sorn);

                    MERGE INTO bk_account_history c
            USING(SELECT TRIM(orchestrate_tran_sn) AS tran_sn,
                        DECODE(orchestrate_sender_id, NULL, 'CNT', orchestrate_sender_id) AS sender_id,
                        a.tmtellid tmtellid,
                        a.tmtxseq tmtxseq,
                        TO_DATE(orchestrate_host_real_date,'YYYYDDD') post_date,
                        LPAD(a.tmacctno, 14, '0')tmacctno,
                        TRIM(a.tmdorc) tmdorc,
                        TRIM(a.tmsseq) tmsseq,
                        TRIM(a.tmhosttxcd)tmhosttxcd
                        FROM sync_cdc_tmtran a
                        WHERE 1=1
                        AND TRIM(a.tmtellid) = orchestrate_teller_id
                        AND TRIM(a.tmtxseq) = orchestrate_host_tran_sn
                        AND a.tmentdt7 =  TO_NUMBER(orchestrate_host_real_date)
                    )src
                    ON( c.teller_id = src.tmtellid
                        AND c.tm_seq = src.tmtxseq
                        AND TRUNC(c.post_time) = src.post_date
                        AND c.rollout_acct_no = tmacctno
                        AND c.dc_sign = TRIM(src.tmdorc)
                        AND c.tran_device = TRIM(src.tmsseq)
                        AND c.device_no = TRIM(src.tmhosttxcd))
               WHEN MATCHED THEN
                    UPDATE
                    SET c.tran_sn = src.tran_sn,
                        c.channel = src.sender_id;
             COMMIT;
            END IF;
        END IF;
         COMMIT;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('RAWSTAGE', 'BEC_MESSAGELOG', 'PR_TRANMAP_SYNC',
                                                    orchestrate_tran_sn, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
*/
/* Dung CDC truc tiep
    PROCEDURE pr_rmdetl_sync (dm_operation_type IN CHAR,
                           orchestrate_rdstat  IN CHAR,
                            orchestrate_rdacct IN NUMBER,
                            orchestrate_rdract  IN NUMBER,
                            orchestrate_rdrtyp  IN VARCHAR2,
                            orchestrate_rdctyp  IN VARCHAR2,
                            orchestrate_rdseq   IN NUMBER,
                            orchestrate_rdbr    IN NUMBER,
                            orchestrate_rdjeff  IN NUMBER,
                            orchestrate_rdjdat  IN NUMBER,
                            orchestrate_rddorc  IN CHAR,
                            orchestrate_rdafft  IN VARCHAR2,
                            orchestrate_rdcode  IN NUMBER,
                            orchestrate_rdamt   IN NUMBER,
                            orchestrate_rdcamt  IN NUMBER,
                            orchestrate_rdcurr  IN CHAR,
                            orchestrate_rdraft  IN VARCHAR2,
                            orchestrate_rduser  IN VARCHAR2,
                            orchestrate_rdauxt  IN VARCHAR2,
                            orchestrate_rdefth  IN VARCHAR2
                            )
    IS
        l_err_desc VARCHAR2(250);
    BEGIN
        IF dm_operation_type <> 'D' AND orchestrate_rdacct > 0 THEN
          MERGE INTO sync_cdc_rmdetl c
                USING(SELECT
                            orchestrate_rdstat AS rdstat ,
                            orchestrate_rdacct AS rdacct,
                            orchestrate_rdract AS rdract ,
                            orchestrate_rdrtyp AS rdrtyp  ,
                            orchestrate_rdctyp AS rdctyp ,
                            orchestrate_rdseq AS rdseq  ,
                            orchestrate_rdbr AS rdbr ,
                            orchestrate_rdjeff AS rdjeff ,
                            orchestrate_rdjdat AS rdjdat,
                            orchestrate_rddorc AS rddorc ,
                            orchestrate_rdafft AS rdafft ,
                            orchestrate_rdcode AS rdcode,
                            orchestrate_rdamt AS rdamt  ,
                            orchestrate_rdcamt AS rdcamt ,
                            orchestrate_rdcurr AS rdcurr ,
                            orchestrate_rdraft AS rdraft ,
                            TRIM(orchestrate_rduser) AS rduser ,
                            TRIM(orchestrate_rdauxt) AS rdauxt ,
                            orchestrate_rdefth  AS rdefth

                            FROM DUAL)a
                         ON(c.rdacct = a.rdacct AND
                            c.rdseq = a.rdseq AND
                            c.rddorc = a.rddorc AND
                            c.rdract = a.rdract AND
                            trim(c.rduser) = a.rduser AND
                            c.rdamt = a.rdamt )
            WHEN MATCHED THEN
                  UPDATE  SET   c.rdstat =  a.rdstat,
                                c.rdrtyp = a.rdrtyp ,
                                c.rdctyp = a.rdctyp  ,
                                c.rdbr = a.rdbr ,
                                c.rdjeff = a.rdjeff  ,
                                c.rdjdat = a.rdjdat ,
                                c.rdafft = a.rdafft ,
                                c.rdcode = a.rdcode ,
                                c.rdcamt = a.rdcamt ,
                                c.rdcurr = a.rdcurr ,
                                c.rdraft = a.rdraft ,
                                c.rdauxt = a.rdauxt ,
                                c.rdefth = a.rdefth

                WHEN NOT MATCHED THEN
                        INSERT( c.rdstat ,
                                c.rdacct  ,
                                c.rdract  ,
                                c.rdrtyp  ,
                                c.rdctyp  ,
                                c.rdseq   ,
                                c.rdbr ,
                                c.rdjeff  ,
                                c.rdjdat  ,
                                c.rddorc  ,
                                c.rdafft  ,
                                c.rdcode  ,
                                c.rdamt   ,
                                c.rdcamt  ,
                                c.rdcurr  ,
                                c.rdraft  ,
                                c.rduser  ,
                                c.rdauxt  ,
                                c.rdefth
                                )
                        VALUES(
                                a.rdstat ,
                                a.rdacct  ,
                                a.rdract  ,
                                a.rdrtyp  ,
                                a.rdctyp  ,
                                a.rdseq   ,
                                a.rdbr ,
                                a.rdjeff  ,
                                a.rdjdat  ,
                                a.rddorc  ,
                                a.rdafft  ,
                                a.rdcode  ,
                                a.rdamt   ,
                                a.rdcamt  ,
                                a.rdcurr  ,
                                a.rdraft  ,
                                a.rduser  ,
                                a.rdauxt  ,
                                a.rdefth );
            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'RMDETL', 'SYNC_CDC_RMDETL',
                                                    orchestrate_rdacct, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
  END;

  */

/* Dung CDC truc tiep
    PROCEDURE pr_rmmast_sync (dm_operation_type IN CHAR,
                              orchestrate_rmtype IN VARCHAR,
                              orchestrate_rmacno  IN NUMBER,
                              orchestrate_rmref   IN VARCHAR,
                              orchestrate_rmbena IN VARCHAR,
                              orchestrate_rmacft IN CHAR,
                              orchestrate_rmcbnk IN VARCHAR,
                              orchestrate_rmibr IN NUMBER,
                              orchestrate_rmpabr IN NUMBER,
                              orchestrate_rmibnk IN VARCHAR,
                              orchestrate_rmpbnk IN VARCHAR,
                              orchestrate_rmis40 IN VARCHAR,
                              orchestrate_rmpb40 IN VARCHAR,
                              orchestrate_rmbcif IN NUMBER)
    IS
        l_err_desc VARCHAR2(250);
    BEGIN
            IF dm_operation_type <> 'D' THEN
                MERGE INTO sync_cdc_rmmast c
                    USING( SELECT  orchestrate_rmacno AS rmacno  ,
                                orchestrate_rmtype AS rmtype,
                                orchestrate_rmref AS rmref,
                                orchestrate_rmbena AS rmbena ,
                                orchestrate_rmacft AS rmacft ,
                                orchestrate_rmcbnk AS rmcbnk,
                                orchestrate_rmibr AS rmibr ,
                                orchestrate_rmpabr AS rmpabr ,
                                orchestrate_rmibnk AS rmibnk ,
                                orchestrate_rmpbnk AS rmpbnk ,
                                orchestrate_rmis40 AS rmis40 ,
                                orchestrate_rmpb40 AS rmpb40,
                                orchestrate_rmbcif AS rmbcif
                                FROM DUAL) a
                    ON (c.rmacno = a.rmacno)
                    WHEN MATCHED
                    THEN
                    UPDATE SET
                    c.rmtype = a.rmtype,
                    c.rmref   = a.rmref,
                    c.rmbena = a.rmbena,
                    c.rmacft = a.rmacft,
                    c.rmcbnk = a.rmcbnk,
                    c.rmibr = a.rmibr,
                    c.rmpabr = a.rmpabr,
                    c.rmibnk = a.rmibnk,
                    c.rmpbnk = a.rmpbnk,
                    c.rmis40 = a.rmis40,
                    c.rmpb40 = a.rmpb40,
                    c.rmbcif = a.rmbcif
                    WHEN NOT MATCHED
                    THEN
                    INSERT( c.rmacno,
                            c.rmtype,
                            c.rmref,
                            c.rmbena,
                            c.rmacft,
                            c.rmcbnk,
                            c.rmibr,
                            c.rmpabr,
                            c.rmibnk,
                            c.rmpbnk,
                            c.rmis40,
                            c.rmpb40,
                            c.rmbcif)
                    VALUES(a.rmacno,
                            a.rmtype,
                            a.rmref,
                            a.rmbena,
                            a.rmacft,
                            a.rmcbnk,
                            a.rmibr,
                            a.rmpabr,
                            a.rmibnk,
                            a.rmpbnk,
                            a.rmis40,
                            a.rmpb40,
                            a.rmbcif);

            COMMIT;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'RMMAST', 'SYNC_CDC_RMMAST',
                                                    orchestrate_rmacno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
*/

/* Dung CDC truc tiep
    PROCEDURE pr_rmtmsg_sync (dm_operation_type IN CHAR,
                              orchestrate_rfacct  IN NUMBER,
                              orchestrate_rfbkrc   IN VARCHAR,
                              orchestrate_rfain IN VARCHAR
                              )
    IS
        l_err_desc VARCHAR2(250);
    BEGIN
            IF dm_operation_type <> 'D' THEN
                MERGE INTO sync_cdc_rmtmsg c
                    USING( SELECT  orchestrate_rfacct AS rfacct ,
                                orchestrate_rfbkrc AS rfbkrc,
                                orchestrate_rfain AS rfain
                                FROM DUAL) a
                    ON (c.rfacct = a.rfacct)
                    WHEN MATCHED
                    THEN
                    UPDATE SET
                    c.rfbkrc   = a.rfbkrc,
                    c.rfain = a.rfain
                    WHEN NOT MATCHED
                    THEN
                    INSERT( c.rfacct,
                            c.rfbkrc,
                            c.rfain)
                    VALUES(a.rfacct,
                            a.rfbkrc,
                            a.rfain );

            COMMIT;
        END IF;


    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'RMTMSG', 'PR_RMTMSG_SYNC',
                                                    orchestrate_rfacct, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
    END;
*/


END;

/
