--------------------------------------------------------
--  DDL for Function FN_GET_ACCOUNT_STATUS_CODE_PRD
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_GET_ACCOUNT_STATUS_CODE_PRD" (p_status VARCHAR2,p_sccode VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE
    IS
        l_status_code VARCHAR2(4);
    BEGIN

        if TRIM(p_sccode)='R-CATCSTK' then 
           l_status_code:= 'VIEW' ; -- View TK ky quy
        ELSE
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
        END IF;
        return l_status_code;


    END;

/
