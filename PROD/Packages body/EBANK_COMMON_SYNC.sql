--------------------------------------------------------
--  DDL for Package Body EBANK_COMMON_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."EBANK_COMMON_SYNC" 
/* Formatted on 12/16/2011 5:05:58 PM (QP5 v5.126) */
 IS
  PROCEDURE proc_update_status_job_sync
  (
    v_status   CHAR,
    v_job_name VARCHAR2
  ) IS
  BEGIN
    UPDATE sync_scan a
    SET    a.date_run = SYSDATE,
           a.status   = v_status
    WHERE  a.job_name = v_job_name;
  
    COMMIT;
    ebank_sync_util.proc_sync_log(SYSDATE,
                                  SYSDATE,
                                  'proc_update_status_job_sync',
                                  '',
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_update_status_job_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          255) */,
                                    'SUCC');
  END;

  PROCEDURE proc_reset_status_send_sync IS
  BEGIN
    UPDATE sync_scan a
    SET    a.is_send = 'N';
  
    COMMIT;
    ebank_sync_util.proc_sync_log(SYSDATE,
                                  SYSDATE,
                                  'proc_reset_status_send_sync',
                                  '',
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_reset_status_send_sync',
                                    'SYSTEM BUSY' /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          255) */,
                                    'SUCC');
  END;

  PROCEDURE proc_scan_job_sync IS
    v_count      NUMBER(2);
    v_running    NUMBER(3);
    v_start_date DATE;
  
    CURSOR c_data IS
      SELECT *
      FROM   sync_scan ss
      WHERE  TRUNC(ss.date_run) = TRUNC(SYSDATE)
      AND    ss.hour_run < (TO_NUMBER(TO_CHAR(SYSDATE,
                                              'hh24')) +
                            (TO_NUMBER(TO_CHAR(SYSDATE,
                                               'mi')) / 60))
      AND    ss.status <> 'Y';
  BEGIN
    v_count      := 0;
    v_start_date := SYSDATE;
  
    --v_job_status
    FOR c_item IN c_data
    LOOP
      IF (c_item.status <> 'Y')
      THEN
        SELECT COUNT(*)
        INTO   v_running
        FROM   user_scheduler_running_jobs a
        WHERE  a.job_name = c_item.job_name;
      
        IF (v_running <= 0)
        THEN
          DBMS_SCHEDULER.run_job(c_item.job_name,
                                 FALSE);
          v_count := 1;
          ebank_sync_util.proc_sync_log(v_start_date,
                                        SYSDATE,
                                        'proc_scan_job_sync',
                                        NULL,
                                        c_item.job_name);
          EXIT WHEN v_count > 0;
        END IF;
      END IF;
    END LOOP;
  
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_scan_job_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_scan_job_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
  END;

  PROCEDURE proc_update_cif_count IS
    v_count      NUMBER;
    v_loop_count NUMBER;
    v_start_date DATE;
  BEGIN
    v_loop_count := 10;
    v_count      := 0;
    v_start_date := SYSDATE;
  
    SELECT a.sync_count
    INTO   v_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';
  
    v_count := v_count + 1000; --dam bao neu buoc sau neu fail
  
    FOR i IN 1 .. v_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        SAVEPOINT s_select_temp;
      
        SELECT MAX(cfcifn)
        INTO   v_count
        FROM   svdatpv51.cfmast@dblink_data;
      
        COMMIT;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_select_temp;
          --DBMS_LOCK.SLEEP(5);
      END;
    END LOOP;
  
    UPDATE sync_checkpoint a
    SET    a.sync_count = v_count
    WHERE  a.sync_type = 'CFMAST';
  
    COMMIT;
  
    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_CIF_COUNT');
  
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_update_cif_count',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_CIF_COUNT');
    
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_update_cif_count',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
  END;

  PROCEDURE proc_del_tran_old_sync IS
    v_start_date DATE;
  BEGIN
    v_start_date := SYSDATE;
  
    DELETE FROM sync_bk_account_history a
    WHERE  trunc(a.post_time) < trunc(SYSDATE - 3);
  
    COMMIT;
  
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_del_tran_old_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_del_tran_old_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
  END;

  PROCEDURE proc_clear_log_sync IS
  BEGIN
    DELETE FROM sync_log a
    WHERE  TRUNC(a.start_date) < TRUNC(SYSDATE - 5);
  
    COMMIT;
  END;

  PROCEDURE proc_reset_scheduler_job_sync IS
  
    CURSOR reset_jobs_cur IS
      SELECT *
      FROM   sync_parameter
      WHERE  code = 'JOB_RESET';
  
  BEGIN
  
    BEGIN
    
      FOR job_rec IN reset_jobs_cur
      LOOP
        DBMS_SCHEDULER.disable(NAME  => job_rec.value,
                               force => TRUE);
      
      
        BEGIN
          DBMS_SCHEDULER.stop_job(job_rec.value,
                                  TRUE);
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line(TO_CHAR(SQLCODE) || ': ' || SQLERRM);
        END;
      
      
        dbms_scheduler.enable(NAME             => job_rec.value,
                              commit_semantics => 'TRANSACTIONAL');
      END LOOP;
    END;
  
    ebank_sync_util.proc_sync_log(SYSDATE,
                                  SYSDATE,
                                  'proc_reset_scheduler_job_sync',
                                  NULL,
                                  'SUCC');
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_reset_scheduler_job_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
    
  END;

  PROCEDURE proc_disable_job_sync IS
  
    CURSOR reset_jobs_cur IS
      SELECT *
      FROM   sync_parameter
      WHERE  code = 'JOB_RESET';
  
  BEGIN
  
    BEGIN
    
      FOR job_rec IN reset_jobs_cur
      LOOP
        BEGIN
          SYS.DBMS_SCHEDULER.DISABLE(NAME  => job_rec.value,
                                     force => TRUE);
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line(TO_CHAR(SQLCODE) || ': ' || SQLERRM);
        END;
      
        BEGIN
          SYS.DBMS_SCHEDULER.STOP_JOB(job_name => job_rec.value,
                                      force    => TRUE);
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line(TO_CHAR(SQLCODE) || ': ' || SQLERRM);
        END;
      
      
      END LOOP;
    END;
  
    ebank_sync_util.proc_sync_log(SYSDATE,
                                  SYSDATE,
                                  'proc_disable_scheduler_job_sync',
                                  NULL,
                                  'SUCC');
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_disable_scheduler_job_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
    
  END;

  PROCEDURE proc_enable_job_sync IS
  
    CURSOR reset_jobs_cur IS
      SELECT *
      FROM   sync_parameter
      WHERE  code = 'JOB_RESET';
  
  BEGIN
  
    BEGIN
    
      FOR job_rec IN reset_jobs_cur
      LOOP
      
        BEGIN
          SYS.DBMS_SCHEDULER.ENABLE(NAME => job_rec.value);
        EXCEPTION
          WHEN OTHERS THEN
            dbms_output.put_line(TO_CHAR(SQLCODE) || ': ' || SQLERRM);
        END;
      
      END LOOP;
    END;
  
    ebank_sync_util.proc_sync_log(SYSDATE,
                                  SYSDATE,
                                  'proc_enable_scheduler_job_sync',
                                  NULL,
                                  'SUCC');
  
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
    
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_enable_scheduler_job_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
    
  END;

END ebank_common_sync;

/
