--------------------------------------------------------
--  DDL for Function IS_NUMBER
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "IBS"."IS_NUMBER" (p_string IN VARCHAR2)
  RETURN INT
IS
  v_num NUMBER;
BEGIN
  v_num := TO_NUMBER(p_string);
  RETURN 1;
EXCEPTION
WHEN VALUE_ERROR THEN
  RETURN 0;
END is_number;

/
