--------------------------------------------------------
--  DDL for Package EBANK_STAGING_TRANSACTION_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."EBANK_STAGING_TRANSACTION_SYNC" is

  -- Author  : QUANLD
  -- Created : 1/10/2014 11:14:03 AM
  -- Purpose : Dong bo lai du lieu tu Staging

  -- Public type declarations
  PROCEDURE proc_run_sync_vipCop;
  Procedure checkSyncDDHist(synDate Date);
  PROCEDURE proc_ddhist_sync_vipCop(p_date DATE);
  PROCEDURE proc_cdhist_bb_vip_sync(p_date DATE);
  PROCEDURE proc_lnhist_bb_vip_sync(p_date DATE);
  PROCEDURE proc_run_test_vipCop;

end EBANK_STAGING_TRANSACTION_SYNC;

/
