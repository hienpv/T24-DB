--------------------------------------------------------
--  DDL for Function FN_GET_ACCOUNT_STATUS_CODE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "IBS"."FN_GET_ACCOUNT_STATUS_CODE" (p_status VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE
    IS
        l_status_code VARCHAR2(4);
    BEGIN
        l_status_code := CASE TRIM (p_status)
                            WHEN '1' THEN 'ACTV'--ACTIVE
                            WHEN '2' THEN 'CLOS'--CLOSED
                            WHEN '3' THEN 'MATU'--MATURED
                            WHEN '4' THEN 'ACTV'--New Record
                            WHEN '5' THEN 'ACTZ'--Active zero balance
                            WHEN '6' THEN 'REST'--RESTRICTED
                            WHEN '7' THEN 'NOPO'--NO POST
                            WHEN '8' THEN 'COUN'--Code unavailable
                            WHEN '9' THEN 'DORM'--DORMANT
                            ELSE ''
                         END;
        return l_status_code;


    END;

/
