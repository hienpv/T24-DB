--------------------------------------------------------
--  DDL for Function FN_GET_APPROVE_HIS_TRANSFER
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_GET_APPROVE_HIS_TRANSFER" (P_WF_PROCESS_ID VARCHAR2)
RETURN VARCHAR2 AS
  V_STATUS VARCHAR2(100);
  V_USERNAME VARCHAR(255);
  cursor c1 is
    select * from IBS_CORP_FLOW.wf_log_info where wf_process_id = P_WF_PROCESS_ID order by start_time desc;
BEGIN

  FOR log_rec in c1
  LOOP
    V_STATUS := log_rec.status;
    IF V_STATUS = 'end process' THEN
      SELECT USER_NAME INTO V_USERNAME FROM bb_user_info where user_id=log_rec.user_id and rownum <= 1;
      RETURN V_USERNAME;
    END IF;
  END LOOP;
  
  RETURN NULL;
END FN_GET_APPROVE_HIS_TRANSFER;

/
