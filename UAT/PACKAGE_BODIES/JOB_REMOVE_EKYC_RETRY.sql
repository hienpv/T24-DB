--------------------------------------------------------
--  DDL for Package Body JOB_REMOVE_EKYC_RETRY
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "JOB_REMOVE_EKYC_RETRY" AS

  PROCEDURE removeEkycRegisterHistoryRetry AS
    BEGIN
      DELETE 
  FROM BC_EKYC_REGISTER_HISTORY_retry where 
  (TO_NUMBER(TO_CHAR(SYSDATE,'HH24'))- TO_NUMBER(TO_CHAR(CREATE_TIME,'HH24')) >=1) or  
 ( TO_NUMBER(TO_CHAR(SYSDATE,'MI'))- TO_NUMBER(TO_CHAR(CREATE_TIME,'MI'))>=15) ;
 
      COMMIT;
    END removeEkycRegisterHistoryRetry;
    

END JOB_REMOVE_ekyc_retry;

/
