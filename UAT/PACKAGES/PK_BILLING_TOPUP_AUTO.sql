--------------------------------------------------------
--  DDL for Package PK_BILLING_TOPUP_AUTO
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PK_BILLING_TOPUP_AUTO" AS 

  /* TODO enter package declarations (types, exceptions, methods etc) here */ 
  PROCEDURE HANDLE_BILLING_TOPUP_AUTO
  (
    P_ACCOUNT_NO VARCHAR2,
    P_PROVIDER VARCHAR2,
    P_PHONE_NUMBER VARCHAR2,
    P_POSTPAID_PHONE_NUMBER VARCHAR2,
    P_AMOUNT NUMBER
  );
END PK_BILLING_TOPUP_AUTO;

/
