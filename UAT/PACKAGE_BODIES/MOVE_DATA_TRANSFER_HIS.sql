--------------------------------------------------------
--  DDL for Package Body MOVE_DATA_TRANSFER_HIS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."MOVE_DATA_TRANSFER_HIS" is

  PROCEDURE moveDataToProd is
    scn_time        varchar2(100);
    scn_time_update varchar2(100);
    --in_host_msg     varchar2(1000);
    check_ex        number;
    count_          number;
    rm_no           varchar2(50);
    cursor transList is
      select *
        from bec.bec_msglog a where  a.IN_HOST_MSG like '%rm_ref_no%' and a.tran_sn > scn_time;
    transItem transList%ROWTYPE;
  begin
    check_ex := 0;
    count_   := 0;
    select a.account_gold_no into scn_time from bc_gold_user a where a.user_id = 2;
    open transList;
    LOOP
      FETCH transList
        INTO transItem;
      EXIT WHEN transList%NOTFOUND;
      begin
        count_ := count_ + 1;
        -- lay rm_no
        rm_no := TO_CHAR(SUBSTR(transItem.IN_HOST_MSG,
                      INSTR(transItem.IN_HOST_MSG, 'rm_ref_no:') + 10,
                      INSTR(SUBSTR(transItem.IN_HOST_MSG,
                                   INSTR(transItem.IN_HOST_MSG, 'rm_ref_no') + 10),
                            ',') - 1));

        select count(1)
          into check_ex
          from bb_transaction_info_bk b
         where b.tran_sn = transItem.message_sn
           and b.rm_no = rm_no;
        if check_ex = 0 then
          -- insert bang bk de theo doi
          insert into bb_transaction_info_bk a
          values
            (transItem.message_sn, rm_no, SYSDATE);

          -- insert sang ben prod
          insert into bb_transaction_info a
          values
            (transItem.message_sn, rm_no, SYSDATE);


        end if;

        scn_time_update := transItem.message_sn;
      exception
        WHEN others THEN
          rollback;
          dbms_output.put_line('Error!');
      end;

    END LOOP;
    CLOSE transList;
    if count_ > 0 then
      update bc_gold_user a
         set a.account_gold_no = scn_time_update
       where a.user_id = 2;
    end if;
    commit;

  end;

  -- add them cac giao d?ch center

 PROCEDURE  moveCenterCorpToProd is
    scn_time        varchar2(100);
    scn_time_update varchar2(100);
    --in_host_msg     varchar2(1000);
    check_ex        number;
    count_          number;
    rm_no           varchar2(50);
    cursor transList is
      select  * from  bec.bec_msglog where Tran_Sn > scn_time and ref_service='SIBS' and resp_code='0' and sender_id='IBS' and in_host_msg like '%rm_ref_no%'
      order by message_sn asc;
    transItem transList%ROWTYPE;
 begin
      check_ex := 0;
      count_   := 0;
      select a.account_gold_no into scn_time from bc_gold_user a where a.user_id = 3;
      open transList;
      LOOP
      FETCH transList
        INTO transItem;
      EXIT WHEN transList%NOTFOUND;
      begin
        count_ := count_ + 1;
          -- lay rm_no
        rm_no := TO_CHAR(SUBSTR(transItem.IN_HOST_MSG,
                      INSTR(transItem.IN_HOST_MSG, 'rm_ref_no:') + 10,
                      INSTR(SUBSTR(transItem.IN_HOST_MSG,
                                   INSTR(transItem.IN_HOST_MSG, 'rm_ref_no') + 10),
                            ',') - 1));

       select count(1)
          into check_ex
          from bb_transaction_info_bk b
         where b.tran_sn = transItem.Tran_Sn
           and b.rm_no = rm_no;
       if check_ex = 0 then
          -- insert bang bk de theo doi
          insert into bb_transaction_info_bk a
          values
            (transItem.Tran_Sn, rm_no, SYSDATE);

          -- insert sang ben prod
          insert into bb_transaction_info a
          values
            (transItem.Tran_Sn, rm_no, SYSDATE);

        end if;
        -- nhap nhat lai scn
        scn_time_update := transItem.message_sn;
      exception
        WHEN others THEN
          rollback;
          dbms_output.put_line('Error!');
      end;

    END LOOP;
    CLOSE transList;
    if count_ > 0 then
      update bc_gold_user a
         set a.account_gold_no = scn_time_update
       where a.user_id = 3;
    end if;

    commit;


 end;

end MOVE_DATA_TRANSFER_HIS;

/
