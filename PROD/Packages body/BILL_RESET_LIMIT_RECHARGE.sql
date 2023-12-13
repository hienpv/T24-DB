--------------------------------------------------------
--  DDL for Package Body BILL_RESET_LIMIT_RECHARGE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."BILL_RESET_LIMIT_RECHARGE" is


  PROCEDURE resetLimitRecharge is
--    cursor schedule_list is select * from bk_tran_schedule a
--    where a.schdule_id in (select b.schdule_id from view_recharge_toup_service b);
--    schedule_item   schedule_list%ROWTYPE;
--    cur_day varchar2(5);

  begin
--     SELECT  EXTRACT (DAY FROM SYSDATE) into cur_day FROM dual;
--     if cur_day = '1' then
--        open schedule_list;
--        LOOP
--                FETCH schedule_list INTO schedule_item;
--                EXIT WHEN schedule_list%NOTFOUND;
--                begin
--                  update bk_tran_schedule a set
--                  a.frq_interval = 0
--                  where a.schdule_id = schedule_item.schdule_id;
--
--                exception
--                   WHEN others THEN
--                     rollback;
--                  dbms_output.put_line('Error!');
--                end;
--
--            END LOOP;
--         CLOSE schedule_list;
--          commit;
--     end if;
    null;
  end;
end BILL_RESET_LIMIT_RECHARGE;

/
