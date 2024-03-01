--------------------------------------------------------
--  DDL for Package EBANK_DWH_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "EBANK_DWH_SYNC" IS
  PROCEDURE proc_ddft_transaction ;
PROCEDURE proc_crtb_tmtran_ol;
 PROCEDURE proc_ddft_transaction_by_date  (p_date DATE);
 PROCEDURE proc_card_statement;
 PROCEDURE proc_card_payment_bk;
END;

/
