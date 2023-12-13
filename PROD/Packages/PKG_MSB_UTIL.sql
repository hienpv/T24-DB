--------------------------------------------------------
--  DDL for Package PKG_MSB_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."PKG_MSB_UTIL" IS

FUNCTION multi_replace_string(pString IN VARCHAR2, pReplacePattern IN VARCHAR2) RETURN VARCHAR2;
FUNCTION multi_replace_clob(pClob IN CLOB, pReplacePattern IN VARCHAR2) RETURN CLOB;

SYS_DEFAULT_PARAM CONSTANT VARCHAR2(20) := ','; -- SUCCESS

END pkg_msb_util;

/
