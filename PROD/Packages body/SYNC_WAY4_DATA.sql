--------------------------------------------------------
--  DDL for Package Body SYNC_WAY4_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."SYNC_WAY4_DATA" IS

 
  PROCEDURE proc_acnt_contract_sync IS

    err varchar2(4000);

  BEGIN

    --delete from SYNC_acnt_contract;
    execute immediate 'truncate table ibs.SYNC_acnt_contract'; --TIENDQ them vao
    
    --delete from SYNC_CIF_WAY4;
    --execute immediate 'truncate table ibs.SYNC_CIF_WAY4';--TIENDQ them vao
    --commit;
    
   /* insert into SYNC_CIF_WAY4
      select bui.cif_no from bc_user_info bui;*/
    --- insert vao bang tam

    insert into ibs.SYNC_acnt_contract
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
      select decode(contr_status, '14', 'ACTV', '63', 'BLOCK', '176','LOBC','266','INAC','64','INAC','NEWR') status,
             id,
             client__id,
             contract_number,
             contract_name,
             acnt_contract__oid,
             check_available,
             msb.xwentry@DBLINK_WAY4('TRANS_CURR', curr) curr,
             billing_contract,
             amount_available,
             amnd_state,
             product,
             total_balance,
             rbs_number,
             sysdate,
             null,
             null,
             msb.rpr.get_av@DBLINK_WAY4(id, curr) available_limit, -- KHANHNN RAO 29052017
             --amount_available available_limit, --KHANHNN ADD 26102017
             -msb.rpr.accntr_get_Credit_limit@DBLINK_WAY4(id) credit_limit,
             F_I,
             SERV_PACK__ID,
             production_status
        from msb.acnt_contract@DBLINK_WAY4
       where amnd_state = 'A'
         /*and client__id in
             (select id
                from msb.client@DBLINK_WAY4
               where client_number in (select f.cif_no from SYNC_CIF_WAY4 f)
                 and amnd_state = 'A')*/
         and contr_status in ('14',
                              '51',
                              '63',--Block Card
                              '64',--Call Issuer
                              '74',--Block Card Lost
                              '85',
                              '97',--PickUp 04
                              '98',--KHOA THE
                              '170',--Block Card By Customer
                              '171',--Card Do not honor - Credit Card
                              '173',--Card No Renewal - Credit Card
                              '174',--Block card Annual fee
                              '175',--WAITING CLOSE
                              '176',--Customer Block
                              '177',--Waiting for Ops activation
                              '266',
                              '287',--PIN Blocked
                              '308',
                              '458',
                              '860'--Block Card Lost
                              ) and ((to_number(to_char(sysdate, 'YYMM')) <= card_expire and CHECK_AVAILABLE='Y') or (CHECK_AVAILABLE='N') or CON_CAT='A');
    --  contr_status  in ('14','63','458','266','64')
    --- insert vao bang that
    commit; -- TIENDQ them vao

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
          commit;

    EXCEPTION
      WHEN OTHERS THEN
        err := sqlerrm;
        insert into SYNC_WAY4_LOG (CREATE_DATE, FUNCTION_ERR, CONTENT)
        values (sysdate, 'Sync_way4_Data.proc_acnt_contract_sync', 'max:' || err);
        commit;
    end;

    commit;
  END proc_acnt_contract_sync;


PROCEDURE proc_acnt_contract_sync_new IS

    err varchar2(4000);
    vcount int;

  BEGIN

    execute immediate 'truncate table ibs.SYNC_acnt_contract'; --TIENDQ them vao    

    insert into ibs.SYNC_acnt_contract
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
      select decode(contr_status, '14', 'ACTV', '63', 'BLOCK', '176','LOBC','266','INAC','64','INAC','NEWR') status,
             id,
             client__id,
             contract_number,
             contract_name,
             acnt_contract__oid,
             check_available,
             msb.xwentry@DBLINK_WAY4('TRANS_CURR', curr) curr,
             billing_contract,
             amount_available,
             amnd_state,
             product,
             total_balance,
             rbs_number,
             sysdate,
             null,
             null,
             msb.rpr.get_av@DBLINK_WAY4(id, curr) available_limit, -- KHANHNN RAO 29052017
             --amount_available available_limit, --KHANHNN ADD 26102017
             -msb.rpr.accntr_get_Credit_limit@DBLINK_WAY4(id) credit_limit,
             F_I,
             SERV_PACK__ID,
             production_status
        from msb.acnt_contract@DBLINK_WAY4
        where amnd_state ='A'
          and amnd_date >= trunc(sysdate) - 3
          and contr_status in ('14',
                              '51',
                              '63',--Block Card
                              '64',--Call Issuer
                              '74',--Block Card Lost
                              '85',
                              '97',--PickUp 04
                              '98',--KHOA THE
                              '170',--Block Card By Customer
                              '171',--Card Do not honor - Credit Card
                              '173',--Card No Renewal - Credit Card
                              '174',--Block card Annual fee
                              '175',--WAITING CLOSE
                              '176',--Customer Block
                              '177',--Waiting for Ops activation
                              '266',
                              '287',--PIN Blocked
                              '308',
                              '458',
                              '860'--Block Card Lost
                              ) and ((to_number(to_char(sysdate, 'YYMM')) <= card_expire and CHECK_AVAILABLE='Y') or (CHECK_AVAILABLE='N') or CON_CAT='A');
                 
          commit; 
          
  Begin  
    for t in (select * from ibs.SYNC_acnt_contract)
                      
    loop
      vcount := 0;
      select count(1) into vcount from ibs.acnt_contract where id = t.id; 
      if (vcount >= 1) then
         --update
         update ibs.acnt_contract
         set status = t.status,
             client__id = t.client__id,
             contract_number = t.contract_number,
             contract_name = t.contract_number,
             acnt_contract__oid = t.acnt_contract__oid,
             check_available = t.check_available,
             curr = t.curr,
             billing_contract = t.billing_contract,
             amount_available = t.amount_available,
             product = t.product,
             total_balance = t.total_balance,
             rbs_number = t.rbs_number,
             previouscyclebalance = t.previouscyclebalance,
             cyclebalance = t.cyclebalance,
             available_limit = t.available_limit,
             credit_limit = t.credit_limit,
             f_i = t.f_i,
             serv_pack__id = t.serv_pack__id,
             production_status = t.production_status, 
             sync_date = sysdate       
         where id = t.id;
         commit;
      else
         --insert
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
                     production_status,
                     sync_date)
        values (t.status,
               t.id,
                t.client__id,
                t.contract_number,
                t.contract_name,
                t.acnt_contract__oid,
                t.check_available,
                t.curr,
                t.billing_contract,
                t.amount_available,
                t.amnd_state,
                t.product,
                t.total_balance,
                t.rbs_number,
                sysdate,
                t.previouscyclebalance,
                t.cyclebalance,
                t.available_limit,
                t.credit_limit,
                t.F_I,
                t.SERV_PACK__ID,
                t.production_status,
                sysdate);
          commit;
      end if;
        
    end loop;
        

    EXCEPTION
      WHEN OTHERS THEN
        err := sqlerrm;
        insert into SYNC_WAY4_LOG (CREATE_DATE, FUNCTION_ERR, CONTENT)
        values (sysdate, 'Sync_way4_Data.proc_acnt_contract_sync', 'max:' || err);
        commit;
    end;

  END proc_acnt_contract_sync_new;


PROCEDURE proc_acnt_contract_sync_manual(p_contract in varchar2, v_msg out varchar2) 
 IS    
    v_count int;
    v_count_search int;
 BEGIN
   
    execute immediate 'truncate table ibs.SYNC_acnt_contract_manual';
    
    insert into ibs.SYNC_acnt_contract_manual
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
      select decode(contr_status, '14', 'ACTV', '63', 'BLOCK', '176','LOBC','266','INAC','64','INAC','NEWR') status,
             id,
             client__id,
             contract_number,
             contract_name,
             acnt_contract__oid,
             check_available,
             msb.xwentry@DBLINK_WAY4('TRANS_CURR', curr) curr,
             billing_contract,
             amount_available,
             amnd_state,
             product,
             total_balance,
             rbs_number,
             sysdate,
             null,
             null,
             msb.rpr.get_av@DBLINK_WAY4(id, curr) available_limit, -- KHANHNN RAO 29052017
             --amount_available available_limit, --KHANHNN ADD 26102017
             -msb.rpr.accntr_get_Credit_limit@DBLINK_WAY4(id) credit_limit,
             F_I,
             SERV_PACK__ID,
             production_status
        from msb.acnt_contract@DBLINK_WAY4
        where amnd_state ='A'
          and contract_number = p_contract
          and contr_status in ('14',
                              '51',
                              '63',--Block Card
                              '64',--Call Issuer
                              '74',--Block Card Lost
                              '85',
                              '97',--PickUp 04
                              '98',--KHOA THE
                              '170',--Block Card By Customer
                              '171',--Card Do not honor - Credit Card
                              '173',--Card No Renewal - Credit Card
                              '174',--Block card Annual fee
                              '175',--WAITING CLOSE
                              '176',--Customer Block
                              '177',--Waiting for Ops activation
                              '266',
                              '287',--PIN Blocked
                              '308',
                              '458',
                              '860'--Block Card Lost
                               ) and ((to_number(to_char(sysdate, 'YYMM')) <= card_expire and CHECK_AVAILABLE='Y') or (CHECK_AVAILABLE='N') or CON_CAT='A');
    commit;
         
    select count(1) into v_count from ibs.SYNC_acnt_contract_manual;    
                                
    if (v_count <= 0) then
         v_msg := 'Ko tim thay contract nay hoac trang thai the ko duoc dong bo hoac the da het han';         
     else
         for t in (select * from ibs.SYNC_acnt_contract_manual)
                     
          LOOP
            v_count_search := 0;
            select count(1) into v_count_search from ibs.acnt_contract where id = t.id; 
            if (v_count_search >= 1) then
               --update
               update ibs.acnt_contract
               set status = t.status,
                   client__id = t.client__id,
                   contract_number = t.contract_number,
                   contract_name = t.contract_number,
                   acnt_contract__oid = t.acnt_contract__oid,
                   check_available = t.check_available,
                   curr = t.curr,
                   billing_contract = t.billing_contract,
                   amount_available = t.amount_available,
                   product = t.product,
                   total_balance = t.total_balance,
                   rbs_number = t.rbs_number,
                   previouscyclebalance = t.previouscyclebalance,
                   cyclebalance = t.cyclebalance,
                   available_limit = t.available_limit,
                   credit_limit = t.credit_limit,
                   f_i = t.f_i,
                   serv_pack__id = t.serv_pack__id,
                   production_status = t.production_status, 
                   sync_date = sysdate       
               where id = t.id;
               commit;
            else
               --insert
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
                           production_status,
                           sync_date)
              values (t.status,
                      t.id,
                      t.client__id,
                      t.contract_number,
                      t.contract_name,
                      t.acnt_contract__oid,
                      t.check_available,
                      t.curr,
                      t.billing_contract,
                      t.amount_available,
                      t.amnd_state,
                      t.product,
                      t.total_balance,
                      t.rbs_number,
                      sysdate,
                      t.previouscyclebalance,
                      t.cyclebalance,
                      t.available_limit,
                      t.credit_limit,
                      t.F_I,
                      t.SERV_PACK__ID,
                      t.production_status,
                      sysdate);
                commit;
            end if;              
          end loop;   
          v_msg := 'Dong bo thanh cong ' || to_char(v_count) || ' ban ghi';        
     end if;                        
         
    EXCEPTION WHEN OTHERS THEN
        v_msg := sqlerrm;        
         
  END proc_acnt_contract_sync_manual;



  /*----------------------
  Dong bo client Way4

  */ -------------------------------------

  PROCEDURE proc_client_sync IS
    err        varchar2(4000);
  BEGIN
    --delete from SYNC_CIF_WAY4;
    execute immediate 'truncate table ibs.SYNC_CIF_WAY4'; --TIENDQ them vao
    --commit;
--    insert into ibs.SYNC_CIF_WAY4
--      select bui.cif_no from bc_user_info bui;

    --delete from SYNC_client;
    execute immediate 'truncate table ibs.SYNC_client'; --TIENDQ them vao
    --commit;
    insert into ibs.SYNC_client
      (id, short_name, address_line_1, CLIENT_NUMBER)
      select id, short_name, address_line_1, CLIENT_NUMBER
        from msb.client@DBLINK_WAY4
       where amnd_state = 'A'
         and client_number in (select f.cif_no from SYNC_CIF_WAY4 f);

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
        insert into SYNC_WAY4_LOG (CREATE_DATE, FUNCTION_ERR, CONTENT)
        values (sysdate, 'Sync_way4_Data.proc_client_sync', 'max:' || err);
        commit;
    END;

  END proc_client_sync;

  PROCEDURE proc_acc_cycle_sync IS

    runDate  varchar2(3);
    billDate date;

  Begin

    billDate := sysdate;
    billDate := to_date('21-04-2022','dd-mm-yyyy');
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
               msb.rpr.Get_Paym_Due_Amount@DBLINK_WAY4(acnt_contract__oid,
                                                       curr,
                                                       trunc(billDate),
                                                       'Y') minPayment,
               msb.rpr.Get_Paym_Due_Date@DBLINK_WAY4(acnt_contract__oid,
                                                     curr,
                                                     trunc(billDate),
                                                     'Y') dueDate,
               null,
               null
          from msb.acc_cycle@DBLINK_WAY4
         where --to_char(date_to,'DD') = '21';
         date_to = trunc(billDate) ;
      commit;
    end if;
  end proc_acc_cycle_sync;

  /*
  Dong bo Appl_Product

  */

  PROCEDURE proc_appl_product_sync IS
    err varchar2(4000);
  Begin
    --delete from ibs.appl_product;
    execute immediate 'truncate table ibs.appl_product'; --TIENDQ them vao
    --commit;
    insert into ibs.appl_product
      (internal_code, name, code, create_date,ACC_SCHEME, amnd_state)
      select internal_code, name, code, sysdate,ACC_SCHEME, amnd_state
        from msb.appl_product@DBLINK_WAY4
       where amnd_state = 'A';

    commit;
  EXCEPTION
    WHEN OTHERS THEN
      err := sqlerrm;
      insert into SYNC_WAY4_LOG (CREATE_DATE, FUNCTION_ERR, CONTENT)
      values (sysdate, 'Sync_way4_Data.proc_appl_product_sync', err);
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
    select max(id) into gMax from msb.doc@DBLINK_WAY4;

    loop
      select max(id) into gMin from doc;
      inext := gMin + cifInteval;
      if (inext > gMax) then
        inext := gMax;
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
          insert into SYNC_WAY4_LOG (CREATE_DATE, FUNCTION_ERR, CONTENT)
          values (sysdate, 'Sync_way4_Data.proc_doc_history_trans_sync', 'max:' || gMax || 'Round: ' || inext || err);
          commit;
      end;
      EXIT WHEN(inext >= gMax);

    end loop;
    commit;
  END proc_doc_history_trans_sync;

/*
PROCEDURE proc_bill_report_sync IS
---add 19.05.2017--
    runDate  varchar2(3);
    billDate date;

  Begin

    --billDate := sysdate;
    --billDate := to_date('21-03-2022','dd-mm-yyyy');
    --select to_char(billDate, 'dd') into runDate from dual;

    --if runDate = '20' or runDate = '21' then
       FOR rec IN (            
                  SELECT  ac.id, ac.client__id, ac.contract_number, mbr.TAD, mbr.mad, mbr.bill_cycle_date, mbr.grace_cycle_date
                     FROM ACNT_CONTRACT ac,
                          creditstm.msb_bill_report@CREDITSTM mbr
                    WHERE ac.contract_number = mbr.contract_number            
           	      )
       LOOP 
            UPDATE ACNT_CONTRACT ac
            SET   ac.minpayment = rec.mad ,
                  ac.cyclebalance = 1,
                  ac.duedate =SYSDATE
            WHERE ac.id = rec.id 
              and ac.client__id = rec.client__id
              and ac.contract_number = rec.contract_number;
        END LOOP;
        commit;
    --end if;
  end proc_bill_report_sync;
  ---------------------------------------------------------------
*/
  PROCEDURE proc_bill_report_sync IS
---add 19.05.2017--
    runDate  varchar2(3);
    billDate date;
    err varchar2(4000);

  Begin

    --billDate := sysdate;
    --billDate := to_date('21-03-2022','dd-mm-yyyy');
    --select to_char(billDate, 'dd') into runDate from dual;
    --if runDate = '20' or runDate = '21' then     
    --delete from SYNC_MSB_BILL_REPORT; 
    execute immediate 'truncate table ibs.SYNC_MSB_BILL_REPORT'; --TIENDQ them vao  
    --commit;
    INSERT INTO SYNC_MSB_BILL_REPORT SELECT * FROM creditstm.msb_bill_report@CREDITSTM mbr;              
    COMMIT;                        
    delete from MSB_BILL_REPORT;

    Begin
      insert into MSB_BILL_REPORT
        select *
          from SYNC_MSB_BILL_REPORT;
    EXCEPTION
      WHEN OTHERS THEN
        err := sqlerrm;
        insert into SYNC_WAY4_LOG (CREATE_DATE, FUNCTION_ERR, CONTENT)
        values (sysdate, 'Sync_way4_Data.proc_acnt_contract_sync', 'max:' || err);
        commit;
    end;

    commit;
        
        
    --end if;
  end proc_bill_report_sync;
  /*
  Dong bo acc_scheme

  */  
    PROCEDURE proc_acc_scheme_sync IS
    err varchar2(4000);
  Begin
    --delete from acc_scheme;
    execute immediate 'truncate table ibs.acc_scheme'; --TIENDQ them vao 
    --commit;
    insert into ibs.acc_scheme
      (amnd_state, id, day_value, due_date)
      select a.amnd_state, a.id, a.day_value, a.due_date
        from msb.acc_scheme@DBLINK_WAY4 a 
       where amnd_state = 'A';

    commit;
  EXCEPTION
    WHEN OTHERS THEN
      err := sqlerrm;
      insert into SYNC_WAY4_LOG (CREATE_DATE, FUNCTION_ERR, CONTENT)
      values (sysdate, 'Sync_way4_Data.proc_acc_scheme_sync', err);
      commit;

  end proc_acc_scheme_sync;
  ---------------------------------------------------------------
  
  /*
  Function chay tat ca cac ham

  */

  PROCEDURE proc_run_way4_sync IS
  BEGIN
    proc_acnt_contract_sync;
    proc_client_sync;
    proc_appl_product_sync;
    --proc_acc_cycle_sync;
    proc_bill_report_sync;
    proc_acc_scheme_sync;
  END proc_run_way4_sync;

end Sync_way4_Data;

/
