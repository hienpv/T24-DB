--------------------------------------------------------
--  DDL for Package PK_UPDATE_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PK_UPDATE_DATA" AS 

  PROCEDURE SYNC_DATA_SWIFT_INFO;
  
  PROCEDURE SYNC_DATA_BENEFIT_RETAIL (
    P_ROLLOUT_ACCT_NO VARCHAR2
  );
END PK_UPDATE_DATA;

/
