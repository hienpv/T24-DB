--------------------------------------------------------
--  DDL for Function FN_CONVERT_MOBILE_TEN_NO
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_CONVERT_MOBILE_TEN_NO" (P_VALUE VARCHAR2)
RETURN VARCHAR2 
AS 
  V_VALUE VARCHAR2(10);
  V_TEMP VARCHAR2(20);
  V_RESULT VARCHAR2(20);
  v_no VARCHAR2(20);
BEGIN

  V_TEMP := replace(P_VALUE, '.', '');
  select substr(V_TEMP, 1, 2) INTO V_VALUE from dual;
  IF V_VALUE = '84' THEN
    V_RESULT := V_TEMP;
  ELSE
    select substr(V_TEMP, 1, 1) INTO V_VALUE from dual;
    IF V_VALUE = '0' THEN
      select '84' || substr(V_TEMP, 2, LENGTH (V_TEMP)) INTO V_RESULT from dual;
      select substr(V_RESULT, 1, 5) INTO v_no from dual;
      IF v_no = '84120' THEN
        select '070' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84121' THEN
        select '079' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84122' THEN
         select '077' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84126' THEN
        select '076' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84128' THEN
        select '078' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84123' THEN
        select '083' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84124' THEN
        select '084' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84125' THEN
        select '085' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84127' THEN
        select '081' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84129' THEN
        select '082' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84162' THEN
        select '032' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84163' THEN
        select '033' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84164' THEN
        select '034' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84165' THEN
        select '035' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84166' THEN
        select '036' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84167' THEN
        select '037' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84168' THEN
        select '038' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84169' THEN
        select '039' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84186' THEN
        select '056' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84188' THEN
        select '058' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSIF v_no = '84199' THEN
        select '059' || substr(V_RESULT, 6, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      ELSE
         select '0' || substr(V_RESULT, 3, LENGTH(V_RESULT) + 1) INTO V_RESULT from dual;
      END IF;
    ELSIF V_VALUE = '+' THEN
      select replace(V_TEMP, '+', '') INTO V_RESULT from dual;
    ELSE
      V_RESULT := V_TEMP;
    END IF;
  END IF;

  RETURN V_RESULT;
END FN_CONVERT_MOBILE_TEN_NO;

/
