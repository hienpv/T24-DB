--------------------------------------------------------
--  DDL for Function FN_GET_CONVERT_MOBILE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "IBS"."FN_GET_CONVERT_MOBILE" (P_VALUE VARCHAR2)
RETURN VARCHAR2 
AS 
  V_VALUE VARCHAR2(100);
  V_TEMP VARCHAR2(200);
  V_RESULT VARCHAR2(200);
BEGIN
  
  V_TEMP := replace(P_VALUE, '.', '');
  select substr(V_TEMP, 1, 2) INTO V_VALUE from dual;
  IF V_VALUE = '84' THEN
    V_RESULT := V_TEMP;
  ELSE
    select substr(V_TEMP, 1, 1) INTO V_VALUE from dual;
    IF V_VALUE = '0' THEN
      select '84' || substr(V_TEMP, 2, LENGTH (V_TEMP)) INTO V_RESULT from dual;
    ELSIF V_VALUE = '+' THEN
      select replace(V_TEMP, '+', '') INTO V_RESULT from dual;
    ELSE
      V_RESULT := V_TEMP;
    END IF;
  END IF;
  
  RETURN V_RESULT;
END FN_GET_CONVERT_MOBILE;

/
