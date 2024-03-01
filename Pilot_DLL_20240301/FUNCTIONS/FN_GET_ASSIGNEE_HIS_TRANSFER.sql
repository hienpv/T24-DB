--------------------------------------------------------
--  DDL for Function FN_GET_ASSIGNEE_HIS_TRANSFER
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "IBS"."FN_GET_ASSIGNEE_HIS_TRANSFER" (P_WF_PROCESS_ID VARCHAR2)
RETURN VARCHAR2 AS
  V_STATUS VARCHAR2(100);
  cursor c1 is
    select * from IBS_CORP_FLOW.wf_log_info where wf_process_id = P_WF_PROCESS_ID order by start_time desc;
BEGIN

  FOR log_rec in c1
  LOOP
    V_STATUS := log_rec.status;
    IF V_STATUS = 'end process' THEN
      RETURN '';
    ELSIF V_STATUS = 'processing' THEN
      RETURN log_rec.assignee;
    END IF;
  END LOOP;

  RETURN NULL;
END FN_GET_ASSIGNEE_HIS_TRANSFER;

/
