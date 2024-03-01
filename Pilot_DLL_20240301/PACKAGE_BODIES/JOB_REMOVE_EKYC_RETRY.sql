--------------------------------------------------------
--  DDL for Package Body JOB_REMOVE_EKYC_RETRY
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "JOB_REMOVE_EKYC_RETRY" AS

  PROCEDURE removeEkycRegisterHistoryRetry AS
    BEGIN
--     delete   BC_EKYC_REGISTER_HISTORY_RETRY  where 
-- 	 to_date(to_char(sysdate,'MM/DD/YYYY'), 'MM/DD/YYYY') -to_date(to_char(CREATE_TIME,'MM/DD/YYYY'),'MM/DD/YYYY') >=3;
--           
--      COMMIT;
    null;
    END removeEkycRegisterHistoryRetry;
    

END JOB_REMOVE_ekyc_retry;

/
