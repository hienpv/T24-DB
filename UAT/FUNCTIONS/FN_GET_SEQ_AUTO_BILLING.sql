--------------------------------------------------------
--  DDL for Function FN_GET_SEQ_AUTO_BILLING
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_GET_SEQ_AUTO_BILLING" 
(
  P_SEQ NUMBER
)
RETURN VARCHAR2 AS
  V_RESUTL VARCHAR2(3);
BEGIN
  V_RESUTL := '000';
  IF LENGTH(P_SEQ) = 1 THEN
    V_RESUTL := '00' || P_SEQ;
  ELSIF LENGTH(P_SEQ) = 2 THEN
    V_RESUTL := '0' || P_SEQ;
  ELSIF LENGTH(P_SEQ) = 3 THEN
    V_RESUTL := TO_CHAR(P_SEQ);
  END IF;
  
  RETURN V_RESUTL;
END FN_GET_SEQ_AUTO_BILLING;

/
