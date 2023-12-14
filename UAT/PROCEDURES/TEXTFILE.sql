--------------------------------------------------------
--  DDL for Procedure TEXTFILE
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "TEXTFILE" 
as
  f UTL_FILE.FILE_TYPE;
  s varchar2(200);  
begin  
  f:=utl_file.fopen('dir','test_pl1.txt','R',200);  
  utl_file.get_line(f,s);
  utl_file.fclose(f);
  dbms_output.put_line(s);
end;

/
