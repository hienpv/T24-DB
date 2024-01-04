--------------------------------------------------------
--  DDL for Package SYNC_WAY4_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."SYNC_WAY4_DATA" is
  PROCEDURE proc_acnt_contract_sync;
  PROCEDURE proc_acnt_contract_sync_new;
  PROCEDURE proc_acnt_contract_sync_manual(p_contract in varchar2, v_msg out varchar2);
  PROCEDURE proc_client_sync;
  PROCEDURE proc_acc_cycle_sync;
  PROCEDURE proc_appl_product_sync;
  PROCEDURE proc_doc_history_trans_sync;
  PROCEDURE proc_bill_report_sync;
  PROCEDURE proc_acc_scheme_sync;
  PROCEDURE proc_run_way4_sync;
end Sync_way4_Data;

/
