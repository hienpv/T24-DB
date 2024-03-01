--------------------------------------------------------
--  DDL for Package PK_PROCESS_FD
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."PK_PROCESS_FD" AS 

  PROCEDURE SYNC_DATA_FD (P_CIF_NO NUMBER);
  
  PROCEDURE SYNC_DATA_FD_BK_ACCOUNT (P_CIF_NO NUMBER);

END PK_PROCESS_FD;

/
