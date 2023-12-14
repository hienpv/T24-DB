--------------------------------------------------------
--  DDL for Function CONVERTFONT
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "CONVERTFONT" (strMessade varchar2) RETURN VARCHAR2 AS
    LANGUAGE JAVA NAME 'Unicode2Nosign.convert(java.lang.String) return java.lang.String';

/
