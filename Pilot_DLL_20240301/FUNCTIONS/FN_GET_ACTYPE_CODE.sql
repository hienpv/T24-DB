--------------------------------------------------------
--  DDL for Function FN_GET_ACTYPE_CODE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "IBS"."FN_GET_ACTYPE_CODE" (p_actype VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE
    IS
        l_actype_code VARCHAR2(10);
    BEGIN
        l_actype_code := CASE TRIM (p_actype)
                            WHEN 'S' THEN 'SA'--Saving
                            ELSE 'CA'
                         END;
        RETURN l_actype_code;

    END;

/
