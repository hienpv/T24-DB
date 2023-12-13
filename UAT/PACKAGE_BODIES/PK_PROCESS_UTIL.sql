--------------------------------------------------------
--  DDL for Package Body PK_PROCESS_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."PK_PROCESS_UTIL" AS

  PROCEDURE proc_update_batch_core AS
  BEGIN
    update IBS.bk_sys_config SET code = 'on'  WHERE  "TYPE" = 'sys_status';
    commit;
  END proc_update_batch_core;

END PK_PROCESS_UTIL;

/
