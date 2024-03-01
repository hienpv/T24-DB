--------------------------------------------------------
--  DDL for Package Body EBANK_SYNC_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."EBANK_SYNC_UTIL" 
/* Formatted on 3-Jun-2011 9:23:23 (QP5 v5.126) */
 IS
  /*ADVICE(4): Unreferenced variable [553] */

  ----------------------------------------------------------------------------
  --16/05/2011
  --Log error
  PROCEDURE proc_sync_log
  (
    p_start_date IN DATE,
    p_end_date   IN DATE,
    p_sync_case  IN VARCHAR2,
    p_error      IN VARCHAR2,
    p_status     IN VARCHAR2
  ) IS
  BEGIN
    INSERT INTO sync_log
      (id,
       start_date,
       end_date,
       sync_case,
       sync_error,
       sync_status)
    VALUES
      (seq_sync_log.NEXTVAL,
       p_start_date,
       p_end_date,
       p_sync_case,
       p_error,
       p_status);
  
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                  SQLERRM,
                                  1,
                                  255));
  END;

  ----------------------------------------------------------------------------
  --Get host status
  --ManhNV
  FUNCTION fc_host_status
  (
    v_host IN VARCHAR2,
    v_user IN VARCHAR2,
    v_pwd  IN VARCHAR2
  ) RETURN VARCHAR2 AS
    LANGUAGE JAVA NAME 'HostStatus.getHostStatus(java.lang.String,java.lang.String,java.lang.String) return java.lang.String';
  ----------------------------------------------------------------------------
  --Get host status
  --ManhNV
  FUNCTION fc_host_status_test RETURN VARCHAR2 AS
    LANGUAGE JAVA NAME 'HostStatus.getHostStatusTest() return java.lang.String';
  ----------------------------------------------------------------------------
  FUNCTION fc_host_status RETURN VARCHAR2 AS
  BEGIN
    RETURN fc_host_status('10.0.1.1',
                          'MSBUSERST',
                          'MSBTEST');
  END;

  PROCEDURE proc_sync_core_status IS
    v_core_status VARCHAR2(20);
    v_core_host   VARCHAR2(20);
    v_core_user   VARCHAR2(20);
    v_core_pwd    VARCHAR2(20);
  BEGIN
  
    SELECT code
    INTO   v_core_host
    FROM   bk_sys_config bsc
    WHERE  bsc."TYPE" = 'core_address';
  
    SELECT code
    INTO   v_core_user
    FROM   bk_sys_config bsc
    WHERE  bsc."TYPE" = 'core_user';
  
    SELECT code
    INTO   v_core_pwd
    FROM   bk_sys_config bsc
    WHERE  bsc."TYPE" = 'core_pwd';
  
    SELECT fc_host_status(v_core_host,
                          v_core_user,
                          v_core_pwd)
    INTO   v_core_status
    FROM   dual;
  
    UPDATE bk_sys_config
    SET    code = DECODE(v_core_status,
                         'READY',
                         'on',
                         'NIGHT',
                         'off')
    WHERE  "TYPE" = 'sys_status';
    --READY=online, NIGHT=offline  
    COMMIT;
    ebank_sync_util.proc_sync_log(SYSDATE,
                                  SYSDATE,
                                  'proc_sync_core_status',
                                  ''
                                  /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                   SQLERRM,
                                                                                                                                                                                   1,
                                                                                                                                                                                   255) */,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_sync_core_status',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                           SQLERRM,
                                                                                                                                                                                           1,
                                                                                                                                                                                           255) */,
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;

END;

/
