--------------------------------------------------------
--  DDL for Package PK_SYNC_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PK_SYNC_DATA" AS 

  PROCEDURE SYNC_DATA_SUMMARY_OFFER (
    P_DATE DATE
  );
  
  PROCEDURE SYNC_DATA_PRODUCT_MASTER (
    P_DATE DATE
  );
  
  PROCEDURE SYNC_DATA_OFFER_SUBSCRIPTION (
    P_DATE DATE
  );
  
  PROCEDURE SYNC_DATA_OFFER_ACCOUNTS (
    P_DATE DATE
  );
  
  PROCEDURE SYNC_DATA_CUSTOMER_MASTER (
    P_DATE DATE
  );
  
  PROCEDURE INSERT_CUSTOMER_MASTER_CIF_NO (
    P_CIF_NO VARCHAR2
  );
END PK_SYNC_DATA;

/
