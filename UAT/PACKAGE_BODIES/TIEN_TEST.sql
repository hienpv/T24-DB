--------------------------------------------------------
--  DDL for Package Body TIEN_TEST
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."TIEN_TEST" IS

  /*
  Dong bo contract
  
  */
  PROCEDURE proc_acnt_contract_sync IS
  
    err varchar2(4000);
  
  BEGIN
  
    delete from SYNC_acnt_contract;
    delete from SYNC_CIF_WAY4;
    commit;
    insert into SYNC_CIF_WAY4
      select bui.cif_no from bc_user_info bui;
    --- insert vao bang tam
  
    insert into SYNC_acnt_contract
      (status,
       id,
       client__id,
       contract_number,
       contract_name,
       acnt_contract__oid,
       check_available,
       curr,
       billing_contract,
       amount_available,
       amnd_state,
       product,
       total_balance,
       rbs_number,
       create_date,
       previouscyclebalance,
       cyclebalance,
       available_limit,
       credit_limit,
       F_I,
       SERV_PACK__ID,
       production_status)
      select decode(contr_status, '14', 'ACTV', '63', 'BLOCK', '170','LOBC','266','INAC','64','INAC','NEWR') status,
             id,
             client__id,
             contract_number,
             contract_name,
             acnt_contract__oid,
             check_available,
             msb.xwentry@dbl_w4_2('TRANS_CURR', curr) curr,
             billing_contract,
             amount_available,
             amnd_state,
             product,
             total_balance,
             rbs_number,
             sysdate,
             null,
             null,
             msb.rpr.get_av@dbl_w4_2(id, curr) available_limit,
             -msb.rpr.accntr_get_Credit_limit@dbl_w4_2(id) credit_limit,
             F_I,
             SERV_PACK__ID,
             production_status
        from msb.acnt_contract@dbl_w4_2
       where amnd_state = 'A'
         and client__id in
             (select id
                from msb.client@dbl_w4_2
               where client_number in (select f.cif_no from SYNC_CIF_WAY4 f)
                 and amnd_state = 'A')
         and contr_status in ('14',
                              '51',
                              '63',
                              '85',
                              '64',
                              '458',
                              '266',
                              '171',
                              '308',
                              '170','287');
    --  contr_status  in ('14','63','458','266','64')
    --- insert vao bang that
  
    delete from acnt_contract;
  
    Begin
      insert into acnt_contract
        (status,
         id,
         client__id,
         contract_number,
         contract_name,
         acnt_contract__oid,
         check_available,
         curr,
         billing_contract,
         amount_available,
         amnd_state,
         product,
         total_balance,
         rbs_number,
         create_date,
         previouscyclebalance,
         cyclebalance,
         available_limit,
         credit_limit,
         F_I,
         SERV_PACK__ID,
         production_status)
        select status,
               id,
               client__id,
               contract_number,
               contract_name,
               acnt_contract__oid,
               check_available,
               curr,
               billing_contract,
               amount_available,
               amnd_state,
               product,
               total_balance,
               rbs_number,
               sysdate,
               previouscyclebalance,
               cyclebalance,
               available_limit,
               credit_limit,
               F_I,
               SERV_PACK__ID,
               production_status
          from SYNC_acnt_contract;
    
    EXCEPTION
      WHEN OTHERS THEN
        err := sqlerrm;
        insert into SYNC_WAY4_LOG
          (CREATE_DATE, FUNCTION_ERR, CONTENT)
        values
          (sysdate,
           'Sync_way4_Data.proc_acnt_contract_sync',
           'max:' || err);
        commit;
    end;
  
    commit;
  END proc_acnt_contract_sync;

  /*----------------------
  Dong bo client Way4
  
  */ -------------------------------------

  PROCEDURE proc_client_sync IS
    err        varchar2(4000);
    cifInteval number(20) := 100000;
    gMin       number(20) := 0;
    gMax       number(20) := 10000000;
    inext      number(20) := 0;
  BEGIN
    delete from SYNC_CIF_WAY4;
    commit;
    insert into SYNC_CIF_WAY4
      select bui.cif_no from bc_user_info bui;
  
    delete from SYNC_client;
    commit;
    insert into SYNC_client
      (id, short_name, address_line_1, CLIENT_NUMBER)
      select id, short_name, address_line_1, CLIENT_NUMBER
        from msb.client@dbl_w4_2
       where id in
             (select max(id)
                from msb.client@dbl_w4_2
               where amnd_state = 'A'
                 and client_number in (select f.cif_no from SYNC_CIF_WAY4 f)
               group by CLIENT_NUMBER);
  
    --- insert vao bang that
  
    delete from client;
    commit;
    Begin
      insert into client
        (id, short_name, address_line_1, create_date, client_number)
        select id, short_name, address_line_1, sysdate, client_number
          from SYNC_client;
    
      commit;
    EXCEPTION
      WHEN OTHERS THEN
        err := sqlerrm;
        insert into SYNC_WAY4_LOG
          (CREATE_DATE, FUNCTION_ERR, CONTENT)
        values
          (sysdate, 'Sync_way4_Data.proc_client_sync', 'max:' || err);
        commit;
    END;
  
  END proc_client_sync;

  PROCEDURE proc_acc_cycle_sync IS
  
    runDate  varchar2(3);
    billDate date;
  
  Begin
  
    billDate := sysdate;
    select to_char(billDate, 'dd') into runDate from dual;
  
    if runDate = '20' or runDate = '21' then
    
      insert into acc_cycle
        (id,
         acnt_contract__oid,
         account__oid,
         acc_templ__id,
         n_of_cycle,
         account_name,
         curr,
         date_from,
         date_to,
         begin_balance,
         item_total,
         fee_total,
         current_balance,
         number_of_docs,
         minPayment,
         dueDate,
         previousCycleBalance,
         cycleBalance)
        select id,
               acnt_contract__oid,
               account__oid,
               acc_templ__id,
               n_of_cycle,
               account_name,
               curr,
               date_from,
               date_to,
               begin_balance,
               item_total,
               fee_total,
               current_balance,
               number_of_docs,
               msb.rpr.Get_Paym_Due_Amount@dbl_w4_2(acnt_contract__oid,
                                                       curr,
                                                       trunc(billDate),
                                                       'Y') minPayment,
               msb.rpr.Get_Paym_Due_Date@dbl_w4_2(acnt_contract__oid,
                                                     curr,
                                                     trunc(billDate),
                                                     'Y') dueDate,
               null,
               null
          from msb.acc_cycle@dbl_w4_2
         where date_to = trunc(billDate);
      commit;
    end if;
  end proc_acc_cycle_sync;

  /*
  Dong bo Appl_Product
  
  */

  PROCEDURE proc_appl_product_sync IS
    err varchar2(4000);
  Begin
    delete from appl_product;
    commit;
    insert into appl_product
      (internal_code, name, code, create_date, amnd_state)
      select internal_code, name, code, sysdate, amnd_state
        from msb.appl_product@dbl_w4_2
       where amnd_state = 'A';
  
    commit;
  EXCEPTION
    WHEN OTHERS THEN
      err := sqlerrm;
      insert into SYNC_WAY4_LOG
        (CREATE_DATE, FUNCTION_ERR, CONTENT)
      values
        (sysdate, 'Sync_way4_Data.proc_appl_product_sync', err);
      commit;
    
  end proc_appl_product_sync;
  ---------------------------------------------------------------
  --Dong b lich su giao dich

  PROCEDURE proc_doc_history_trans_sync IS
  
    cifInteval number(20) := 10000;
    gMin       number(20) := 0;
    gMax       number(20) := 10000000;
    inext      number(20) := 0;
    err        varchar2(4000);
    docMaxId   number(20);
  
  BEGIN
    select max(id) into gMax from msb.doc@dbl_w4_2;
  
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
                 msb.xwentry@dbl_w4_2('TRANS_CURR', TRANS_CURR) curr,
                 TARGET_NUMBER,
                 sysdate
            from msb.doc@dbl_w4_2
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
  END proc_doc_history_trans_sync;

  ---------------------------------------------------------------

  /*
  Function chay tat ca cac ham
  
  */

  PROCEDURE proc_run_way4_sync IS
  BEGIN
    proc_acnt_contract_sync;
    proc_client_sync;
    proc_appl_product_sync;
    proc_acc_cycle_sync;
  END proc_run_way4_sync;

end TIEN_TEST;

/
