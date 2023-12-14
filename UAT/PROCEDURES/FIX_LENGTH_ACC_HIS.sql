--------------------------------------------------------
--  DDL for Procedure FIX_LENGTH_ACC_HIS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "FIX_LENGTH_ACC_HIS" is
BEGIN
  
  FOR i IN 4 .. 6  
  LOOP BEGIN
  
  execute EBANK_TRANSACTION_SYNC.proc_ddhist_by_date_sync(sysdate-i);
  
  END;
  
  END LOOP;
END;

/
