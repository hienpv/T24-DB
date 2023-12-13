--------------------------------------------------------
--  DDL for Package PK_MIGRATE_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."PK_MIGRATE_DATA" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
  PROCEDURE P_BB_USER_PERMISSION_UPGRADE (
    P_MODULE_ID NUMBER,
    P_ROLE_ID NUMBER
  );
END PK_MIGRATE_DATA;

/
