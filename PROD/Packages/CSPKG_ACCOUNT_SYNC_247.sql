--------------------------------------------------------
--  DDL for Package CSPKG_ACCOUNT_SYNC_247
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKG_ACCOUNT_SYNC_247" AS 

  /* 
    author: QuangBD3
  */ 
  
  PROCEDURE pr_update_cif_change (p_old_cif NUMBER, p_new_cif NUMBER, p_acct_no VARCHAR2); 

  
  PROCEDURE pr_ddmemo_sync_247 (dm_operation_type in CHAR,
                                orchestrate_before_cifno IN NUMBER, 
                                orchestrate_cifno IN NUMBER, 
                                orchestrate_acctno IN NUMBER,
                                orchestrate_status IN NUMBER,
                                orchestrate_hold IN NUMBER,
                                orchestrate_cbal IN NUMBER,
                                orchestrate_odlimt IN NUMBER,
                                orchestrate_acname IN VARCHAR2,
                                orchestrate_dla7 IN NUMBER
                                );
END CSPKG_ACCOUNT_SYNC_247;

/
