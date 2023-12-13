--------------------------------------------------------
--  DDL for Package PK_UAT_INIT_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."PK_UAT_INIT_DATA" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
  PROCEDURE SET_DATA_BK_SYS_PARAMETER(
    P_DATE DATE
  );

  PROCEDURE SET_DATA_BK_SYS_PARAMETER(
    P_USERNAME VARCHAR2,
    P_START_NO NUMBER,
    P_END_NO NUMBER
  );
END PK_UAT_INIT_DATA;

/
