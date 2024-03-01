--------------------------------------------------------
--  DDL for Function FN_GET_TRAN_SERVICE_CODE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_GET_TRAN_SERVICE_CODE" (p_tran_service_code VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE
    IS
        l_tran_service_code VARCHAR2(10);
    BEGIN
        SELECT  DECODE(p_tran_service_code,
                                            NULL,
                                            'RTR001',
                                            'RTR002')
        INTO l_tran_service_code
        FROM DUAL;

        RETURN l_tran_service_code;
    END;

/
