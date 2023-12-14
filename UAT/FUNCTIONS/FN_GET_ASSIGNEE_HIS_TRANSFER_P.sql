--------------------------------------------------------
--  DDL for Function FN_GET_ASSIGNEE_HIS_TRANSFER_P
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_GET_ASSIGNEE_HIS_TRANSFER_P" (P_WF_PROCESS_ID VARCHAR2)
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
END FN_GET_ASSIGNEE_HIS_TRANSFER_P;


INSERT INTO IBS_CORP_FLOW.wf_bb_assignee_super(assignee_id,
    process_id,
    process_level,
    corp_id,
    user_id,
    user_name,
    create_by,
    create_time,
    process_definition_key)
SELECT
    assignee_id,
    process_id,
    process_level,
    corp_id,
    user_id,
    user_name,
    create_by,
    create_time,
    process_definition_key
FROM
    IBS.wf_bb_assignee_super;

;
INSERT INTO IBS_CORP_FLOW.wf_log_info(log_id,
    wf_process_id,
    apply_name,
    status,
    start_time,
    end_time,
    remarks,
    assignee,
    sys_code,
    user_id,
    process_id)

SELECT
    log_id,
    wf_process_id,
    apply_name,
    status,
    start_time,
    end_time,
    remarks,
    assignee,
    sys_code,
    user_id,
    process_id
FROM
    IBS.wf_log_info;

select * from bb_second_user where user_name='sap_test';

/
