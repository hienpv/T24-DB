--------------------------------------------------------
--  DDL for Package EBANK_TRANSACTION_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."EBANK_TRANSACTION_SYNC" IS

  PROCEDURE proc_tmtran_onday_sync;

  PROCEDURE proc_tmtran_onday_fail_sync;

  PROCEDURE proc_salary_onday_sync;

  PROCEDURE proc_salary_cif_sync;

  PROCEDURE proc_salary_hist_sync;

  PROCEDURE proc_ddhist_trancode_sync;

  PROCEDURE proc_transfer_schedule_sync;

  PROCEDURE proc_tmtran_hist_sync;

  PROCEDURE proc_tmtran_cif_sync;

  PROCEDURE proc_ddhist_cif_sync;

  PROCEDURE proc_cdhist_cif_sync;

  PROCEDURE proc_lnhist_cif_sync;

  PROCEDURE proc_all_info_cif_sync(v_type VARCHAR2);

  PROCEDURE proc_reset_sequence_sync;

  PROCEDURE proc_ddhist_sync;

  PROCEDURE proc_cdhist_sync;

  PROCEDURE proc_lnhist_sync;

  PROCEDURE proc_ddhist_by_date_acct_sync
  (
    p_acct_no VARCHAR2,
    p_date    DATE
  );

  PROCEDURE proc_ddhist_by_date_sync(p_date DATE);

  PROCEDURE proc_cdhist_by_date_acct_sync
  (
    p_acct_no VARCHAR2,
    p_date    DATE
  );

  PROCEDURE proc_cdhist_by_date_sync(p_date DATE);

  PROCEDURE proc_lnhist_by_date_acct_sync
  (
    p_acct_no VARCHAR2,
    p_date    DATE
  ); 
	
	PROCEDURE proc_lnhist_by_date_sync(p_date DATE);

END;

/
