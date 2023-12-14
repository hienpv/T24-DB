--------------------------------------------------------
--  DDL for Function DISPLAY_RAW
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "IBS"."DISPLAY_RAW" (rawval RAW, TYPE VARCHAR2) RETURN VARCHAR2 IS cn NUMBER; CV VARCHAR2 (4000); cd DATE; cnv NVARCHAR2 (2000); cr ROWID; cc CHAR (4000); BEGIN IF (TYPE = 'NUMBER') THEN DBMS_STATS.convert_raw_value (rawval, cn); RETURN TO_CHAR (cn); ELSIF (TYPE = 'VARCHAR2') THEN DBMS_STATS.convert_raw_value (rawval, CV); RETURN TO_CHAR (CV); ELSIF (TYPE = 'DATE') THEN DBMS_STATS.convert_raw_value (rawval, cd); RETURN TO_CHAR (cd); ELSIF (TYPE = 'NVARCHAR2') THEN DBMS_STATS.convert_raw_value (rawval, cnv); RETURN TO_CHAR (cnv); ELSIF (TYPE = 'ROWID') THEN DBMS_STATS.convert_raw_value (rawval, cr); RETURN TO_CHAR (cnv); ELSIF (TYPE = 'CHAR') THEN DBMS_STATS.convert_raw_value (rawval, cc); RETURN TO_CHAR (cc); ELSE RETURN 'UNKNOWN DATATYPE'; END IF; END;

/
