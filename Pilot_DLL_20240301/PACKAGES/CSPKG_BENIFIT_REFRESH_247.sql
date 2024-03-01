--------------------------------------------------------
--  DDL for Package CSPKG_BENIFIT_REFRESH_247
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKG_BENIFIT_REFRESH_247" 
  IS
    PROCEDURE pr_benifit_internal_txn;

    PROCEDURE pr_benifit_internal_txn_on_day;

    PROCEDURE pr_load_tw_txn(p_batch_no NUMBER);

    PROCEDURE pr_unload_tw_txn(p_batch_no NUMBER);

END;

/
