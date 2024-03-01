--------------------------------------------------------
--  DDL for Package Body CSPKG_ACCOUNT_SYNC_247
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."CSPKG_ACCOUNT_SYNC_247" AS

PROCEDURE pr_update_cif_change (p_old_cif NUMBER, p_new_cif NUMBER, p_acct_no VARCHAR2)
    IS
        l_check NUMBER(10) := 0;
        l_acc_type VARCHAR2(30);
    BEGIN

            --20160830 QuanPD Added: Cap nhat nhung tai khoan thay doi CIF
            --#20160921 Loctx move from ddmast_sync
            IF (NVL(p_old_cif, 0) <> p_new_cif) THEN
                SELECT CASE
                           WHEN EXISTS
                                    (SELECT 1
                                       FROM (
                                             SELECT cif_no FROM bb_corp_info) a
                                      WHERE a.cif_no = TO_CHAR (p_new_cif))
                           THEN 1
                           ELSE 0
                       END
                  INTO l_check
                  FROM DUAL;

--                IF l_check = 0--new CIF khong dang ky EBANK      --#20160921 Loctx add
--                THEN
--                    UPDATE bc_related_account c SET c.status = 'DLTD'
--                    WHERE c.acct_no = p_acct_no
--                    AND EXISTS(
--                        SELECT 1 FROM bc_user_info b
--                        WHERE TO_CHAR (p_old_cif) = trim(b.cif_no)
--                        AND c.user_id = b.user_id
--                    );
-- 
--
--                ELSE -- new cif da dang ky EBANK
--                    SELECT acct_type
--                    INTO l_acc_type
--                    FROM bk_account_info
--                    WHERE acct_no = p_acct_no;
--
--                   MERGE INTO bc_related_account c
--                        USING(SELECT
--                                    p_acct_no acctno,
--                                    --l_acc_status status,
--                                    l_acc_type actype,
--                                    b.user_id
--                                FROM  bc_user_info b
--                                WHERE TO_CHAR (p_new_cif) = trim(b.cif_no)
--                                AND b.status = 'ACTV'
--                        )src
--                        ON(src.acctno = c.acct_no)
--                   WHEN MATCHED
--                   THEN
--                       UPDATE SET 
--                               -- 20171011 QuanDH :  Khong thay doi status  voi status= VIEW 
--                               -- c.status = 'ACTV',
--                                  c.status =  CASE  WHEN  c.status = 'VIEW' 
--                                         THEN  c.status   ELSE   'ACTV'  END ,
--                                  c.user_id = src.user_id
--                   WHEN NOT MATCHED
--                   THEN
--                        INSERT(c.relation_id,
--                                    c.user_id,
--                                    c.acct_no,
--                                    c.acct_type,
--                                    c.is_master,
--                                    c.status,
--                                    c.create_time)
--                           VALUES(seq_relation_id.NEXTVAL,
--                                     src.user_id,
--                                     src.acctno,
--                                     src.actype,
--                                     'N',
--                                     'ACTV',
--                                     SYSDATE);
--                END IF;
            END IF;
    END;
    
  PROCEDURE pr_ddmemo_sync_247 (dm_operation_type in CHAR,
                                orchestrate_before_cifno IN NUMBER, 
                                orchestrate_cifno IN NUMBER, 
                                orchestrate_acctno IN NUMBER,
                                orchestrate_status IN NUMBER,
                                orchestrate_hold IN NUMBER,
                                orchestrate_cbal IN NUMBER,
                                orchestrate_odlimt IN NUMBER,
                                orchestrate_acname IN VARCHAR2,
                                orchestrate_dla7 IN NUMBER
                                ) AS
        l_acc_status VARCHAR2 (10);
        l_acc_no VARCHAR2(14) ;
        l_err_desc VARCHAR2(250);
        v_code_cut_off VARCHAR2(50);
  BEGIN
    -- select trim(code)  into v_code_cut_off from bk_sys_config where type = 'sys_status';
    -- Trong gio chay batch thi moi thuc hien procedure
     -- IF v_code_cut_off = 'off' THEN
        cspks_cdc_util.pr_log_sync_error('SIBS', 'DDME24', 'BK_ACCOUNT_INFO',
                                                     orchestrate_acctno, dm_operation_type, 
                                                     orchestrate_cbal
                             - orchestrate_hold
                            + orchestrate_odlimt
                             ); 
                                                      
          IF dm_operation_type <> 'D' THEN
              IF TRIM (orchestrate_status) IS NOT NULL THEN
                  l_acc_status := fn_get_account_status_code (orchestrate_status);
                  l_acc_no := (case when length(orchestrate_acctno) = 13 then LPAD (orchestrate_acctno, 14, '0') else TO_CHAR(orchestrate_acctno) end);
                   
                  UPDATE bk_account_info c
                     SET 
                        -- 20171011 QuanDH :  Khong thay doi status  voi status= VIEW 
                         -- c.status = l_acc_status,
                         c.status =  CASE  WHEN  c.status = 'VIEW' 
                                           THEN  c.status   ELSE   l_acc_status  END ,
                         c.hold_amount = orchestrate_hold,
                         c.ledger_balance = orchestrate_cbal,
                         c.available_balance = orchestrate_cbal
                             - orchestrate_hold
                             + orchestrate_odlimt,
                         c.overdraft_limit = orchestrate_odlimt,
                         c.acct_name = TRIM (orchestrate_acname),
                         c.cif_no = orchestrate_cifno
                  WHERE c.acct_no = l_acc_no;
  
                  --#20160921 lOCTX ADD CIF CHANGE
                  IF orchestrate_before_cifno IS NOT NULL AND orchestrate_before_cifno <> orchestrate_cifno THEN
                      pr_update_cif_change(orchestrate_before_cifno, orchestrate_cifno, l_acc_no);
                  END IF;
  
                  COMMIT;
  
              END IF;
  
  
          END IF;
     -- END IF; 
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);

            cspks_cdc_util.pr_log_sync_error('SIBS', 'DDME24', 'BK_ACCOUNT_INFO',
                                                    orchestrate_acctno, dm_operation_type, l_err_desc);
            COMMIT;
            RAISE;
  END pr_ddmemo_sync_247;
  

END CSPKG_ACCOUNT_SYNC_247;

/
