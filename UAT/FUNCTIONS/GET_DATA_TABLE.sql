--------------------------------------------------------
--  DDL for Function GET_DATA_TABLE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "GET_DATA_TABLE" (p_USER_ID NUMBER)
    return BB_USER_INFO_TYPE_tb is
    PRAGMA AUTONOMOUS_TRANSACTION; 
      v_emptype BB_USER_INFO_TYPE_tb := BB_USER_INFO_TYPE_tb(); 
      v_username VARCHAR2(100 BYTE);
      v_nick VARCHAR2(100 BYTE) ;
      v_cnt NUMBER(19,0):=0;
      v_rc      SYS_REFCURSOR;
  begin
  
    insert into cstb_cdc_log
    select * from cstb_cdc_log
    where unique_id_in_source ='4001010073978' and trunc(log_date) ='29-MAY-21' and rownum =1;
    commit;
    
    v_rc := get_user_info_reports(p_USER_ID); 
    loop
     fetch v_rc into v_username, v_nick;
     exit when v_rc%NOTFOUND; 
      v_emptype.extend;
      v_cnt := v_cnt + 1;
      v_emptype(v_cnt) := BB_USER_INFO_TYPE(v_username, v_nick);
    end loop;
   close v_rc;
   return v_emptype;
 end;

/
