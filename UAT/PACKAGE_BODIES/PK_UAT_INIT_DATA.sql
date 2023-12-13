--------------------------------------------------------
--  DDL for Package Body PK_UAT_INIT_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."PK_UAT_INIT_DATA" AS

  PROCEDURE SET_DATA_BK_SYS_PARAMETER(
    P_DATE DATE
  ) AS
  BEGIN
  
    UPDATE BK_SYS_PARAMETER 
    SET 
      NAME = to_char(P_DATE, 'ddMMyy') 
    WHERE TYPE = 'date_core_uat' AND CHANNEL_CODE = 'IB';
    
    COMMIT;
    
  END SET_DATA_BK_SYS_PARAMETER;

  PROCEDURE SET_DATA_BK_SYS_PARAMETER(
    P_USERNAME VARCHAR2,
    P_START_NO NUMBER,
    P_END_NO NUMBER
  ) AS
  BEGIN
    null;
  END;
END PK_UAT_INIT_DATA;

/
