--------------------------------------------------------
--  DDL for Package Body PK_UPDATE_STATUS_LOAN
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_UPDATE_STATUS_LOAN" AS

  PROCEDURE updateStatusLoan AS
   BEGIN
--     update BC_LOAN_CREDIT_APPROVAL set APPROVAL_STATUS='EXPIRED' WHERE  WITHDRAWAL_EXPIRED_DATE < (SYSDATE);
--              commit;
--    commit;  
    null;
    END updateStatusLoan;   

END PK_UPDATE_STATUS_LOAN;

/
