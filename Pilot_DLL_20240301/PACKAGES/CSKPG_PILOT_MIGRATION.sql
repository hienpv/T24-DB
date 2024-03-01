--------------------------------------------------------
--  DDL for Package CSKPG_PILOT_MIGRATION
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSKPG_PILOT_MIGRATION" AS 

  PROCEDURE cd_mast_migration;
  
  PROCEDURE dd_mast_migration;
  
  PROCEDURE ln_mast_migration;
  
  PROCEDURE bk_cif_migration;
  
  PROCEDURE bk_account_history_change_type;


END CSKPG_PILOT_MIGRATION;

/
