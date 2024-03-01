--------------------------------------------------------
--  DDL for Package Body PKG_MSB_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."PKG_MSB_UTIL" IS


FUNCTION multi_replace_string(pString IN VARCHAR2, pReplacePattern IN VARCHAR2) RETURN VARCHAR2 IS
    iCount  INTEGER;
    vResult VARCHAR2(1000);
    vRule   VARCHAR2(1000);
    vOldStr VARCHAR2(500);
    vNewStr VARCHAR2(500);
BEGIN
    iCount := 0;
    vResult := pString;
    LOOP
        iCount := iCount + 1;

        -- Step # 1: Pick out the replacement rules
        vRule := REGEXP_SUBSTR(pReplacePattern, '[^' || SYS_DEFAULT_PARAM || ']+', 1, iCount);

        -- Step # 2: Pick out the old and new string from the rule
        vOldStr := REGEXP_SUBSTR(vRule, '[^=]+', 1, 1);
        vNewStr := REGEXP_SUBSTR(vRule, '[^=]+', 1, 2);

        -- Step # 3: Do the replacement
        vResult := REPLACE(vResult, vOldStr, vNewStr);

        EXIT WHEN vRule IS NULL;
    END LOOP;

    RETURN vResult;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line(SQLERRM);
        RETURN NULL;
    END;

FUNCTION multi_replace_clob(pClob IN CLOB, pReplacePattern IN VARCHAR2) RETURN CLOB IS
    iCount  INTEGER;
    vResult CLOB;
    vRule   VARCHAR2(1000);
    vOldStr VARCHAR2(1000);
    vNewStr VARCHAR2(1000);
BEGIN
    iCount := 0;
    vResult := pClob;
    LOOP
        iCount := iCount + 1;

        -- Step # 1: Pick out the replacement rules
        vRule := REGEXP_SUBSTR(pReplacePattern, '[^' || SYS_DEFAULT_PARAM || ']+', 1, iCount);

        -- Step # 2: Pick out the old and new string from the rule
        vOldStr := REGEXP_SUBSTR(vRule, '[^=]+', 1, 1);
        vNewStr := REGEXP_SUBSTR(vRule, '[^=]+', 1, 2);

        -- Step # 3: Do the replacement
        vResult := REPLACE(vResult, vOldStr, vNewStr);

        EXIT WHEN vRule IS NULL;
    END LOOP;

    RETURN vResult;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line(SQLERRM);
        RETURN NULL;
    END;
END;

/
