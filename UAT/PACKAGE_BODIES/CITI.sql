--------------------------------------------------------
--  DDL for Package Body CITI
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CITI" as
  function p_string return varchar2 is
  begin
    return 'Greating!';
  end p_string;
  --make sum  
  function tsum(t1 number, t2 number) return number
  is
    kt number:=0;
  begin
    kt:= t1+t2 + 9;
    return kt;
  end tsum;
  
end citi;

/
