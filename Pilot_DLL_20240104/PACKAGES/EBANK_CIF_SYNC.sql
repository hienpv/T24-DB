--------------------------------------------------------
--  DDL for Package EBANK_CIF_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."EBANK_CIF_SYNC" IS
  PROCEDURE proc_mobile_sync;
  PROCEDURE proc_email_sync;
  PROCEDURE proc_telephone_sync;
  PROCEDURE proc_cif_info_sync;
  --PROCEDURE proc_mobile_cif_sync;
  --PROCEDURE proc_email_cif_sync;
  --PROCEDURE proc_telephone_cif_sync;
  --PROCEDURE proc_all_cif_sync;
  PROCEDURE proc_all_address_cif_sync;
  --PROCEDURE proc_all_address_by_cif_sync;
  PROCEDURE proc_reset_checkpoint;
END;

/
