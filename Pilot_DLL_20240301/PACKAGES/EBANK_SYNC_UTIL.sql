--------------------------------------------------------
--  DDL for Package EBANK_SYNC_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "EBANK_SYNC_UTIL" IS
  PROCEDURE proc_sync_log
  (
    p_start_date IN DATE,
    p_end_date   IN DATE,
    p_sync_case  IN VARCHAR2,
    p_error      IN VARCHAR2,
    p_status     IN VARCHAR2
  );
  FUNCTION fc_host_status
  (
    v_host IN VARCHAR2,
    v_user IN VARCHAR2,
    v_pwd  IN VARCHAR2
  ) RETURN VARCHAR2;
  FUNCTION fc_host_status RETURN VARCHAR2;
  FUNCTION fc_host_status_test RETURN VARCHAR2;
  PROCEDURE proc_sync_core_status;
END;

/
