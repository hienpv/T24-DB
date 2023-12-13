--------------------------------------------------------
--  DDL for Package Body PK_WORKFLOW_PROCESS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_WORKFLOW_PROCESS" AS

  PROCEDURE PROCESS_CONFIG_WORKFLOW(P_CIF_NO in varchar2, P_PROCESS_DEFINITION_KEY in varchar2) AS
    V_CORP_ID NUMBER;
    V_DEF_PROCESS_ID NUMBER;
    V_PROCESS_ID NUMBER;
  BEGIN
    
    select CORP_ID INTO V_CORP_ID from IBS.BB_CORP_INFO where cif_no = P_CIF_NO and status='ACTV';
    IF V_CORP_ID IS NOT NULL THEN
        select IBS_CORP_FLOW.seq_PROCESS_id.nextval INTO V_PROCESS_ID from dual;
        
        select PROCESSID INTO V_DEF_PROCESS_ID FROM IBS_CORP_FLOW.WF_BB_DEFINITION where PROCESS_DEFINITION_KEY = P_PROCESS_DEFINITION_KEY; 
        
        UPDATE IBS_CORP_FLOW.WF_BB_PROCESS_UPGRADE SET STATUS = 'DLTD'
        where CORP_ID = V_CORP_ID and STATUS='ACTV' and PROCESS_DEFINITION_KEY in (P_PROCESS_DEFINITION_KEY);
        
        INSERT INTO IBS_CORP_FLOW.WF_BB_PROCESS_UPGRADE 
        (PROCESS_ID, def_process_id, corp_id, process_definition_key, service_name, process_level, min_amount, max_amount,
        create_by,create_time,is_limit, is_order) 
        values (V_PROCESS_ID, V_DEF_PROCESS_ID, V_CORP_ID,P_PROCESS_DEFINITION_KEY,P_PROCESS_DEFINITION_KEY,1,0,99999999999999999999,-1,sysdate, 0, 1);
        commit;
        
        FOR workflow_rec IN (
          select USER_ID, USER_NAME from IBS.BB_USER_INFO where CORP_ID = V_CORP_ID and status != 'DLTD' and role_id = 3
        )
        LOOP
          insert into IBS_CORP_FLOW.WF_BB_ASSIGNEE_UPGRADE (ASSIGNEE_ID, PROCESS_ID, PROCESS_LEVEL, 
          CORP_ID, USER_ID, USER_NAME, 
          CREATE_BY, CREATE_TIME, PROCESS_DEFINITION_KEY
          )
        values (IBS_CORP_FLOW.SEQ_ASSIGNEE_ID.nextval, V_PROCESS_ID, 1, 
          V_CORP_ID, workflow_rec.USER_ID, workflow_rec.USER_NAME,  -1, sysdate, P_PROCESS_DEFINITION_KEY
          );
        END LOOP;
        
    END IF;
    commit;
  END PROCESS_CONFIG_WORKFLOW;
  
  PROCEDURE PROCESS_CONFIG_WORKFLOW_2_LEV (
    P_CIF_NO in varchar2, 
    P_PROCESS_DEFINITION_KEY in varchar2,
    P_ASSIGNEE_LEVEL1 in varchar2,
    P_ASSIGNEE_LEVEL2 in varchar2
  ) AS
    V_CORP_ID NUMBER;
    V_DEF_PROCESS_ID NUMBER;
    V_PROCESS_ID NUMBER;
    V_USER_ID NUMBER;
  BEGIN
    
    select CORP_ID INTO V_CORP_ID from IBS.BB_CORP_INFO where cif_no = P_CIF_NO and status='ACTV';
    IF V_CORP_ID IS NOT NULL THEN
        select IBS_CORP_FLOW.seq_PROCESS_id.nextval INTO V_PROCESS_ID from dual;
        
        select PROCESSID INTO V_DEF_PROCESS_ID FROM IBS_CORP_FLOW.WF_BB_DEFINITION where PROCESS_DEFINITION_KEY = P_PROCESS_DEFINITION_KEY; 
        
        UPDATE IBS_CORP_FLOW.WF_BB_PROCESS_UPGRADE SET STATUS = 'DLTD'
        where CORP_ID = V_CORP_ID and STATUS='ACTV' and PROCESS_DEFINITION_KEY in (P_PROCESS_DEFINITION_KEY);
        
        INSERT INTO IBS_CORP_FLOW.WF_BB_PROCESS_UPGRADE 
        (PROCESS_ID, def_process_id, corp_id, process_definition_key, service_name, process_level, min_amount, max_amount,
        create_by,create_time,is_limit, is_order) 
        values (V_PROCESS_ID, V_DEF_PROCESS_ID, V_CORP_ID,P_PROCESS_DEFINITION_KEY,P_PROCESS_DEFINITION_KEY,2,0,99999999999999999999,-1,sysdate, 0, 1);
        commit;
        
        FOR workflow1_rec IN (
            select regexp_substr (
                   P_ASSIGNEE_LEVEL1,
                   '[^,]+',
                   1,
                   level
                 ) USER_NAME
          from   dual
          connect by level <= 
            length ( trim ( both ',' from P_ASSIGNEE_LEVEL1 ) ) - 
            length ( replace ( P_ASSIGNEE_LEVEL1, ',' ) ) + 1
        )
        LOOP
            select USER_ID INTO V_USER_ID from IBS.BB_USER_INFO where lower(user_name) = lower(workflow1_rec.USER_NAME) and status='ACTV';
            
            insert into IBS_CORP_FLOW.WF_BB_ASSIGNEE_UPGRADE (ASSIGNEE_ID, PROCESS_ID, PROCESS_LEVEL, 
              CORP_ID, USER_ID, USER_NAME, CREATE_BY, CREATE_TIME, PROCESS_DEFINITION_KEY
              )
            values (IBS_CORP_FLOW.SEQ_ASSIGNEE_ID.nextval, V_PROCESS_ID, 1, 
              V_CORP_ID, V_USER_ID, workflow1_rec.USER_NAME,  -1, sysdate, P_PROCESS_DEFINITION_KEY
              );
        END LOOP;
        
        FOR workflow2_rec IN (
            select regexp_substr (
                   P_ASSIGNEE_LEVEL2,
                   '[^,]+',
                   1,
                   level
                 ) USER_NAME
          from   dual
          connect by level <= 
            length ( trim ( both ',' from P_ASSIGNEE_LEVEL2 ) ) - 
            length ( replace ( P_ASSIGNEE_LEVEL2, ',' ) ) + 1
        )
        LOOP
            select USER_ID INTO V_USER_ID from IBS.BB_USER_INFO where lower(user_name) = lower(workflow2_rec.USER_NAME) and status='ACTV';
            
            insert into IBS_CORP_FLOW.WF_BB_ASSIGNEE_UPGRADE (ASSIGNEE_ID, PROCESS_ID, PROCESS_LEVEL, 
              CORP_ID, USER_ID, USER_NAME, CREATE_BY, CREATE_TIME, PROCESS_DEFINITION_KEY
              )
            values (IBS_CORP_FLOW.SEQ_ASSIGNEE_ID.nextval, V_PROCESS_ID, 2, 
              V_CORP_ID, V_USER_ID, workflow2_rec.USER_NAME,  -1, sysdate, P_PROCESS_DEFINITION_KEY
              );
        END LOOP;
        
    END IF;
    commit;
  END PROCESS_CONFIG_WORKFLOW_2_LEV;

END PK_WORKFLOW_PROCESS;

/
