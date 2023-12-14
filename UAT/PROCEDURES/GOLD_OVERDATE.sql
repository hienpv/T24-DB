--------------------------------------------------------
--  DDL for Procedure GOLD_OVERDATE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "GOLD_OVERDATE" AS
BEGIN


update bc_gold_withdraw_register set status = 'OVER' where regis_date + 1 <= sysdate and status = 'NEWR';

commit;
END GOLD_OVERDATE;

/
