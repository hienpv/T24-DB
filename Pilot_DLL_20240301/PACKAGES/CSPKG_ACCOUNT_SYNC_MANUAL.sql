--------------------------------------------------------
--  DDL for Package CSPKG_ACCOUNT_SYNC_MANUAL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKG_ACCOUNT_SYNC_MANUAL" is

  -- Author  : DUANNP2
  -- Created : 5/22/2023 1:42:37 PM
  -- Purpose : 
  
  -- Public type declarations
  PROCEDURE pr_ddmemo_sync;
  PROCEDURE pr_ddmemo_sync_single (p_acct_no VARCHAR2);
  
  PROCEDURE pr_cdmemo_sync;
  PROCEDURE pr_cdmemo_sync_single (p_acct_no VARCHAR2);
  
  PROCEDURE pr_ddtnew_sync;
  
  PROCEDURE pr_cdtnew_sync;

end CSPKG_ACCOUNT_SYNC_MANUAL;

/
