--------------------------------------------------------
--  DDL for Function FN_GET_TRANSACTION_BITCOIN
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_GET_TRANSACTION_BITCOIN" (P_VALUE VARCHAR2) 
RETURN NUMBER AS 
  V_RESULT NUMBER;
BEGIN
    select count(ID) INTO V_RESULT from bc_infringe_keywords where keyword in (
    select regexp_substr(trim(LOWER(P_VALUE)),'[^ ]+', 1, level) from dual
    connect by regexp_substr(trim(LOWER(P_VALUE)), '[^ ]+', 1, level) is not null);
  RETURN V_RESULT;
END FN_GET_TRANSACTION_BITCOIN;

/
