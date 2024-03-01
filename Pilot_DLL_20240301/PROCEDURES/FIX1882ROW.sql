--------------------------------------------------------
--  DDL for Procedure FIX1882ROW
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "FIX1882ROW" (own VARCHAR2,
                                         tn VARCHAR2,
                                         cn VARCHAR2,
                                          r ROWID) as
  res varchar2(1000);
  q number(10);
  stmt varchar2(1000);
  tmz varchar2(100);
  tstz timestamp with time zone;
  adj interval day to second;
  lck EXCEPTION;
  PRAGMA EXCEPTION_INIT(lck, -54);
begin
--  tmz := 'none';
--  stmt := 'SELECT DUMP("' || cn || '",1016) FROM "' || own || '"."';
--  stmt := stmt || tn || '" WHERE ROWID = ''' || r || ''' FOR UPDATE NOWAIT';
--  execute immediate stmt into res;
--  q := 0;
--  for i in reverse 1..length(res) loop
--    if substr(res,i,1) = ',' then
--      if q = 0 then
--        q:=i;
--      else
--        q:=i;
--        exit;
--      end if;
--    end if;
--  end loop;
--  res := upper(substr(res,q+1));
--  CASE
--    WHEN res = '97,8' THEN tmz := 'HST';
--    WHEN res = '99,90' THEN tmz := 'EST5EDT'; --This was EST
--    WHEN res = 'A9,98' THEN tmz := 'MST7MDT'; --This was MST
--    WHEN res = '91,90' THEN tmz := 'EST5EDT';
--    WHEN res = 'A1,98' THEN tmz := 'MST7MDT';
--    WHEN res = '91,94' THEN tmz := 'CST6CDT';
--    WHEN res = '91,9C' THEN tmz := 'PST8PDT';
--    ELSE DBMS_OUTPUT.PUT_LINE('Unknown time zone "'||res||'" in '||own|| --
--                              '.'||tn||'('||cn||') '||r);
--  END CASE;
--  if tmz <> 'none' then
--    stmt := 'UPDATE "' || own || '"."' || tn || '" SET "' || cn ;
--    stmt := stmt || '" = "' || cn || '" AT TIME ZONE ''';
--    stmt := stmt || tmz || ''' WHERE ROWID = ''' || r || '''';
--    execute immediate stmt;
--      DBMS_OUTPUT.PUT_LINE('Row successfully modified: '||own||'.'||tn||'('||cn||') '||r);
--    adjust_tz (own, tn, cn, r);
--  end if;
--exception
--  when lck then
--    DBMS_OUTPUT.PUT_LINE('Row locked: '||own||'.'||tn||'('||cn||') '||r);
--  when others then
--    DBMS_OUTPUT.PUT_LINE('Unknown error on: '||own||'.'||tn||'('||cn||') '||r);
    null;
end;

/
