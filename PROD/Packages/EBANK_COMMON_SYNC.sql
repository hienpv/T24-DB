--------------------------------------------------------
--  DDL for Package EBANK_COMMON_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."EBANK_COMMON_SYNC" IS

  PROCEDURE proc_scan_job_sync;

  PROCEDURE proc_update_status_job_sync
  (
    v_status   CHAR,
    v_job_name VARCHAR2
  );

  PROCEDURE proc_update_cif_count;

  PROCEDURE proc_clear_log_sync;

  PROCEDURE proc_del_tran_old_sync;

  PROCEDURE proc_reset_status_send_sync;

  PROCEDURE proc_reset_scheduler_job_sync;

  PROCEDURE proc_enable_job_sync;

  PROCEDURE proc_disable_job_sync;

END EBANK_COMMON_SYNC;

/
