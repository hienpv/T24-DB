--------------------------------------------------------
--  DDL for Procedure PRT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PRT" (str1 in varchar2)
is  
  name0 varchar2(50);
  name1 varchar(50):='Trong Khiem';
begin
    name0:='Dao Cuong';
    DBMS_OUTPUT.PUT_line(name0 || ',' || name1 || ' is ' || str1);
    dbms_output.put_line('-----######--------');
    dbms_output.put_line('is sum: '|| getsum1(7,8));    
    
    dbms_output.put_line(CITI.P_STRING);
    dbms_output.put_line(citi.tsum(7,5));
    --DBMS_LOCK.SLEEP (5); 
end;

/
