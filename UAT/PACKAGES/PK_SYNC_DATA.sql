--------------------------------------------------------
--  DDL for Package PK_SYNC_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PK_SYNC_DATA" AS 

  PROCEDURE SYNC_DATA_SAVING_ONLINE(
    P_DATE DATE
  );
  
  
END PK_SYNC_DATA;

/
