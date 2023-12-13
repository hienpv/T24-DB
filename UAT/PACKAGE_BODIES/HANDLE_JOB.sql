--------------------------------------------------------
--  DDL for Package Body HANDLE_JOB
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "HANDLE_JOB" AS

  PROCEDURE check_job AS
  v_last_start_date date;
  v_state varchar2(100);
  BEGIN
    for cur in ( select * from user_scheduler_jobs 
                where upper(job_action) like '%EBANK_ACCOUNT_SYNC%')
    loop 
         select LAST_START_DATE, STATE      into v_last_start_date, v_state
             from user_scheduler_jobs     where job_name = cur.JOB_NAME;
             
         if ( v_last_start_date < trunc(sysdate) and  v_state = 'RUNNING') then
            dbms_output.put_line('job need restart: ' || cur.JOB_NAME);
            dbms_scheduler.stop_job(cur.JOB_NAME,force=>true);
         end if;
    end loop;
  END check_job;

END HANDLE_JOB;

/
