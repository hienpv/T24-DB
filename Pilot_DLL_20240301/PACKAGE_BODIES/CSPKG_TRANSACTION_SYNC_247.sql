--------------------------------------------------------
--  DDL for Package Body CSPKG_TRANSACTION_SYNC_247
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_TRANSACTION_SYNC_247" AS

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
                    FROM bk_account_history_247 c,
                        sync_cdc_bk_acct_hist_fail_247 a
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

                l_taget_table := 'BK_ACCOUNT_HISTORY_247';
                UPDATE bk_account_history_247 c
                    SET c.status = l_txn.status,
                        c.tran_service_code = l_txn.tran_service_code,
                        c.channel = l_txn.channel,
                        c.remark = l_txn.remark
                WHERE ROWID = l_txn.hist_rowid;

                l_taget_table := 'sync_cdc_bk_acct_hist_fail_247';
                UPDATE sync_cdc_bk_acct_hist_fail_247
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

            cspks_cdc_util.pr_log_sync_error('SIBS', 'TTR24_BACK_FAIL', l_taget_table,
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
                                orchestrate_tmacctno in NUMBER,
                                orchestrate_tmtellid in VARCHAR,
                                orchestrate_tmtxseq in NUMBER,
                                orchestrate_tmtxstat in VARCHAR,
                                orchestrate_tmhosttxcd in NUMBER,
                                orchestrate_tmapptype in CHAR,
                                orchestrate_tmeqvtrn in CHAR,
                                orchestrate_tmibttrn in CHAR,
                                orchestrate_tmsumtrn in CHAR,
                                orchestrate_tmentdt7 in NUMBER,
                                orchestrate_tmeffdt7 in NUMBER,
                                orchestrate_tmsseq in NUMBER,
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
             l_acct_no := LPAD(orchestrate_tmacctno, 14, '0');

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

                l_taget_table := 'BK_ACCOUNT_HISTORY_247';

                UPDATE bk_account_history_247 c
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
                    INSERT INTO sync_cdc_bk_acct_hist_fail_247 c(c.core_sn,
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
                                                        TO_DATE(orchestrate_tmresv07,'yyyyMMdd:hh24miss') AS tran_date,
                                                        TO_DATE(orchestrate_tmresv07,'yyyyMMdd:hh24miss') AS post_date,
                                                        TRIM(orchestrate_tmdorc) AS tmdorc,
                                                        orchestrate_tmtxamt AS tmtxamt,
                                                        TRIM(orchestrate_tmglcur) AS tmglcur,
                                                        --orchestrate_tmorgamt AS tmorgamt,
                                                        null,
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

                COMMIT;
            END IF;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1,200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'TTR24_FAIL', l_taget_table,
                                                    orchestrate_tmacctno, dm_operation_type, l_err_desc);

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
                                orchestrate_tmacctno in NUMBER,
                                orchestrate_tmtellid in VARCHAR,
                                orchestrate_tmtxseq in NUMBER,
                                orchestrate_tmtxstat in VARCHAR,
                                orchestrate_tmhosttxcd in NUMBER,
                                orchestrate_tmapptype in CHAR,
                                orchestrate_tmeqvtrn in CHAR,
                                orchestrate_tmibttrn in CHAR,
                                orchestrate_tmsumtrn in CHAR,
                                orchestrate_tmentdt7 in NUMBER,
                                orchestrate_tmeffdt7 in NUMBER,
                                orchestrate_tmsseq in NUMBER,
                                orchestrate_tmtiment in NUMBER)
    IS
        l_check_exists NUMBER := 0;
        l_acct_no VARCHAR2(20);
        l_err_desc VARCHAR2(250);
        l_tmtran_fail CHAR(1) := 'N';
        l_taget_table VARCHAR2(100);

        l_tran_sn bk_account_history_247.tran_sn%type;
        l_sender_id VARCHAR2(50);
        l_status VARCHAR2(10);
        l_acct  NUMBER := 0;
    BEGIN

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

            l_acct_no := LPAD(orchestrate_tmacctno, 14, '0');

            -- Neu giao dich duoc thuc hien boi Ebank thi khong can check CIF co su dung Ebank hay khong
            IF orchestrate_tmtellid like 'EBANKING3%'
                OR orchestrate_tmtellid like 'EBANKING4%'
                OR orchestrate_tmtellid like 'EBANKING5%'
                OR TRIM(orchestrate_tmapptype) = 'T' --#20150508 LOctx add: TK CD khong check, xu ly truong hop cdtnew den sau tmtran
            THEN
                l_check_exists := 1;
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
            END IF;

            SELECT  DECODE(TRIM(orchestrate_tmtxstat),
                                                'CE',
                                                'FAIL',
                                                'SUCC')
            INTO l_status
            FROM DUAL;

            IF(orchestrate_tmtxstat = 'CE' --giao dich huy --#20141026 Loctx add: xu ly job TMTRAN_FAIL
                    AND orchestrate_tmhosttxcd NOT IN (179)
            )
            THEN
                l_tmtran_fail := 'Y';
            END IF;

      -- Luu giao dich gio chay batch trong BK_ACCOUNT_HISTORY_TMTR24 (tru GD247)
      IF orchestrate_tmtellid not like 'IBSML247%' AND orchestrate_tmtellid not like 'IBDN247%' THEN
        INSERT INTO BK_ACCOUNT_HISTORY_TMTR24 c
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
             c.trace_code,
                         c.tmentdt7,
                         c.tmtiment)
        SELECT
            TRIM(orchestrate_tmresv07) || seq_core_sn.NEXTVAL,
            TO_DATE(orchestrate_tmresv07,'yyyyMMdd:hh24miss') AS tran_date,
            TO_DATE(orchestrate_tmresv07,'yyyyMMdd:hh24miss') AS post_date,
            TRIM(orchestrate_tmdorc) AS tmdorc,
            orchestrate_tmtxamt AS tmtxamt,
            TRIM(orchestrate_tmglcur) AS tmglcur,
            --orchestrate_tmorgamt AS tmorgamt,
            null,
            --#20151001 LocTx change from --DECODE(l_sender_id, NULL, 'CNT', l_sender_id) AS sender_id,
            (CASE WHEN orchestrate_tmtellid LIKE '%EBANK%' THEN 'IBS' ELSE 'CNT' END) AS sender_id,
            --SUBSTR(orchestrate_tmefth, 11, LENGTH(orchestrate_tmefth)) AS tmefth,
            -- nang cap thay doi noi dung cho trancd 160
            DECODE(orchestrate_tmhosttxcd,'160','Tra lai TK KKH', SUBSTR(orchestrate_tmefth, 11, LENGTH(orchestrate_tmefth))) AS tmefth,
            orchestrate_tmacctno AS tmacctno,
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
            TRIM(orchestrate_tmtxcd) AS tmtxcd,
                        orchestrate_tmentdt7,
                        orchestrate_tmtiment
        FROM DUAL
        LEFT JOIN sync_tran_code d ON TRIM(orchestrate_tmtxcd) = d.tran_code;
      END IF;

            IF l_check_exists = 1 AND l_tmtran_fail = 'N' THEN --haons add 20150410
                --#20150804 Loctx add
                BEGIN
                    SELECT MAX(tran_sn), MAX(sender_id)
                    INTO l_tran_sn, l_sender_id
                    FROM  sync_cdc_tranmap b
                    WHERE  b.teller_id = TRIM(orchestrate_tmtellid)
                    AND    b.host_tran_sn = orchestrate_tmtxseq --TO_CHAR(orchestrate_tmtxseq)--#20150527 Loctx tuniing, chang to number
                    AND    b.host_real_date = orchestrate_tmentdt7--TO_CHAR(orchestrate_tmentdt7)--#20150527 Loctx tuniing, chang to number
                    AND b.sorn = 'Y'; --#20150526 LocTX add

                EXCEPTION--#20150425 Loctx add check exception
                    WHEN NO_DATA_FOUND THEN
                        NULL;
                END;
                    l_taget_table := 'BK_ACCOUNT_HISTORY_247';

                    INSERT INTO bk_account_history_247 c
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
                    SELECT
                             TRIM(orchestrate_tmresv07) || seq_core_sn.NEXTVAL,
                                TO_DATE(orchestrate_tmresv07,'yyyyMMdd:hh24miss') AS tran_date,
                                TO_DATE(orchestrate_tmresv07,'yyyyMMdd:hh24miss') AS post_date,
                                TRIM(orchestrate_tmdorc) AS tmdorc,
                                orchestrate_tmtxamt AS tmtxamt,
                                TRIM(orchestrate_tmglcur) AS tmglcur,
                                --orchestrate_tmorgamt AS tmorgamt,
                                null,
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
                    LEFT JOIN sync_tran_code d ON TRIM(orchestrate_tmtxcd) = d.tran_code
                    ;

                COMMIT;
            END IF;
        END IF;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'TTR24', l_taget_table,
                                                    orchestrate_tmacctno, dm_operation_type, l_err_desc);

            COMMIT;
            RAISE;
    END;


END CSPKG_TRANSACTION_SYNC_247;

/
