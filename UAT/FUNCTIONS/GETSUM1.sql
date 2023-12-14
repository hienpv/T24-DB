--------------------------------------------------------
--  DDL for Function GETSUM1
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "GETSUM1" (a1 number, a2 number)
return number
is
  --t1 number;
  --t2 number;
  tkq number:=0;
begin
  DBMS_OUTPUT.PUT('----SUM----');
  --a1:=7;a2:=8;
  tkq:= a1+a2;
  return tkq;
  
end;

/
