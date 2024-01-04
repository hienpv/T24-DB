--------------------------------------------------------
--  DDL for Package CSPKG_BALANCE_PERIOD_SYNC_TEMP
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKG_BALANCE_PERIOD_SYNC_TEMP" 
  IS
--
-- To modify this template, edit file PKGSPEC.TXT in TEMPLATE
-- directory of SQL Navigator
--
-- Purpose: Briefly explain the functionality of the package
--
-- MODIFICATION HISTORY
-- Person      Date     Comments
-- QuanPD     20150820  Added
   -- Enter package declarations as shown below

    PROCEDURE pr_ddmemo_later_process;

      PROCEDURE pr_cdmemo_later_process;

END; -- Package spec

/
