--------------------------------------------------------
--  DDL for Package EBANK_ACCOUNT_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "EBANK_ACCOUNT_SYNC" IS

  PROCEDURE proc_ddmast_sync;

  --PROCEDURE proc_ddmast_cif_sync;
  PROCEDURE proc_cdmast_sync;

  PROCEDURE proc_cdgroup_sync;

  PROCEDURE proc_cdgroup_onday_sync;

  --PROCEDURE proc_cdmast_cif_sync;
  PROCEDURE proc_lnmast_sync;

  --PROCEDURE proc_lnmast_cif_sync;
  PROCEDURE proc_ddtnew_sync;

  PROCEDURE proc_cdtnew_sync;

  PROCEDURE proc_lntnew_sync;

  PROCEDURE proc_ddmemo_sync;

  PROCEDURE proc_cdmemo_sync;

  PROCEDURE proc_lnmemo_sync;

  PROCEDURE proc_passbook_no_sync;

  --PROCEDURE proc_account_cif_sync;

  PROCEDURE proc_ddmaster;
  PROCEDURE proc_cdmaster;
  PROCEDURE proc_lnmaster;
 

END;

/
