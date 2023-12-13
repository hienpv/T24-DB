--------------------------------------------------------
--  DDL for Package Body CSPKG_TRANSACTION_SYNC_BUG
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_TRANSACTION_SYNC_BUG" 
IS 
  
    PROCEDURE  pr_tmtran_sync (dm_operation_type in CHAR,
                                orchestrate_tmtxcd in VARCHAR,
                                orchestrate_tmresv07 in VARCHAR,
                                orchestrate_tmdorc in CHAR,
                                orchestrate_tmtxamt in NUMBER,
                                orchestrate_tmglcur in VARCHAR,
                                orchestrate_tmorgamt in NUMBER,
                                orchestrate_tmefth in VARCHAR2,
                                orchestrate_tmacctno in VARCHAR,  
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
 cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'BEGIN pr_tmtran_sync' );  

CSPKS_CDC_T24_UTIL.PR_T24_CDD_LOG_TMTRAN ( dm_operation_type , orchestrate_tmtxcd   ,   orchestrate_tmresv07 ,   orchestrate_tmdorc   ,
                                  orchestrate_tmtxamt  , orchestrate_tmglcur  , orchestrate_tmorgamt , orchestrate_tmefth   ,   orchestrate_tmacctno , 
                                  orchestrate_tmtellid ,  orchestrate_tmtxseq  ,  orchestrate_tmtxstat ,  orchestrate_tmhosttxcd ,  orchestrate_tmapptype  ,
                                  orchestrate_tmeqvtrn   , orchestrate_tmibttrn   ,  orchestrate_tmsumtrn   , orchestrate_tmentdt7   ,     orchestrate_tmeffdt7   ,
                                  orchestrate_tmsseq     , orchestrate_tmtiment   , 0 , 0 , 'CDD INPUT BUG' ) ;

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
       cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', 'NA',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     'pr_tmtran_sync : dm_operation_type = I...' );  
            l_acct_no := LPAD(orchestrate_tmacctno, 14, '0');


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
                    l_taget_table := 'BK_ACCOUNT_HISTORY';
                     cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN', l_taget_table,
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     ' INSERT pr_tmtran_sync' ); 
               OPEN l_history;
                LOOP
                FETCH  l_history  INTO c_history;
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

                COMMIT;
            END IF;
        END IF;
            cspks_cdc_util.pr_log_sync_error('SIBS', 'TMTRAN',  '',
                                                     orchestrate_tmacctno, dm_operation_type, 
                                                     ' END pr_tmtran_sync' );
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



END;

/
