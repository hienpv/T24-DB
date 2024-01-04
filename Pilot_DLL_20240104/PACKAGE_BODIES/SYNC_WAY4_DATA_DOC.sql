--------------------------------------------------------
--  DDL for Package Body SYNC_WAY4_DATA_DOC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."SYNC_WAY4_DATA_DOC" IS

  PROCEDURE proc_doc_history_trans_sync_2 IS

    cifInteval number(20) := 10000;
    gMin       number(20) := 0;
    gMax       number(20) := 10000000;
    inext      number(20) := 0;
    err        varchar2(4000);
    docMaxId   number(20);

  BEGIN
    select max(id) into gMax from msb.doc@DBLINK_WAY4;

    loop
      select max(id) into gMin from doc;
      inext := gMin + cifInteval;
      if (inext > 100000000) then
        inext := 100000000;
      end if;
      Begin

        --- insert vao bang tam
        insert into doc
          (ID,
           TRANS_DATE,
           trans_amount,
           trans_city,
           trans_details,
           TRANS_TYPE,
           posting_date,
           curr,
           TARGET_NUMBER,
           create_date)
          select ID,
                 TRANS_DATE,
                 trans_amount,
                 trans_city,
                 trans_details,
                 TRANS_TYPE,
                 posting_date,
                 msb.xwentry@DBLINK_WAY4('TRANS_CURR', TRANS_CURR) curr,
                 TARGET_NUMBER,
                 sysdate
            from msb.doc@DBLINK_WAY4
           where ((TRANS_TYPE in ('35542', '146') and return_code = 0) or
                 (trans_type in ('5', '13') and IS_AUTHORIZATION = 'N' and
                 return_code = 0) or
                 (TRANS_TYPE = '15' and return_code = 101))
             and AMND_STATE = 'A'
             and id <= inext
             and id > gmin;

        commit;
      EXCEPTION
        WHEN OTHERS THEN
          err := sqlerrm;
          insert into SYNC_WAY4_LOG
            (CREATE_DATE, FUNCTION_ERR, CONTENT)
          values
            (sysdate,
             'Sync_way4_Data.proc_doc_history_trans_sync',
             'max:' || gMax || 'Round: ' || inext || err);
          commit;
      end;
      EXIT WHEN(inext >= gMax);

    end loop;
    commit;
  END proc_doc_history_trans_sync_2;

end Sync_way4_Data_doc;

/
