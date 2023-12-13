--------------------------------------------------------
--  DDL for Package CITI
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CITI" as
  function p_string return varchar2;
  function tsum(t1 number, t2 number)  return number;

end;

/
