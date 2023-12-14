--------------------------------------------------------
--  DDL for Function FN_GET_ACCOUNT_SYNC_SPECIAL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "FN_GET_ACCOUNT_SYNC_SPECIAL" (p_product_type VARCHAR2, p_type VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE
    IS
        l_result VARCHAR2(100) default '';
        l_acct_type VARCHAR2(100) default '';
        l_status VARCHAR2(100) default '';
        l_product_format VARCHAR2(100) default '';
    BEGIN
        select acct_type, status, product_format
        into  l_acct_type, l_status, l_product_format 
        from bk_account_product_sync aps
        where aps.product_type in (p_product_type);
        IF p_type='S' THEN
            l_result := l_status;
        ELSIF p_type='A' THEN   
            l_result := l_acct_type;
        ELSE
            l_result := l_product_format;
        END IF;

        RETURN l_result;

    END;

/
