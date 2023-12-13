--------------------------------------------------------
--  DDL for Package Body BILL_PAYMENT_BEGIN_DAY
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "BILL_PAYMENT_BEGIN_DAY" is

       
  PROCEDURE scanBillPaymentBeginDay is
  run_time varchar2(20); 
  start_record varchar2(20);
  end_record number(9);
  get_record_count varchar2(20);
  row_count number(9);
  cursor bill_list is select * from view_bill_payment_begin_date 
  where rowNum_ >= TO_NUMBER(start_record) and  rowNum_ < end_record;
  bill_item   bill_list%ROWTYPE;   
  
  begin
    
   
   select a.name into run_time  from bk_sys_parameter a where a.type = 'pagation_bill' and a.code = 'RD'; 
   select a.name into start_record  from bk_sys_parameter a where a.type = 'pagation_bill' and a.code = 'FR'; 
   select a.name into get_record_count  from bk_sys_parameter a where a.type = 'pagation_bill' and a.code = 'RC'; 
   if run_time <> to_char(sysdate, 'yyyyMMdd') then
       DBMS_OUTPUT.PUT_LINE( 'start job!');  
       -- clean data in bc_bill_payment_query
      if start_record = '0' then 
         execute immediate 'truncate table bc_bill_payment_query' ;
      end if;
      
      select count(1) into row_count from view_bill_payment_begin_date;
      DBMS_OUTPUT.PUT_LINE( 'row_count: ' || to_char(row_count)); 
      end_record := TO_NUMBER(start_record) + TO_NUMBER(get_record_count);

      open  bill_list; 
      
        LOOP
            FETCH bill_list INTO bill_item;
            EXIT WHEN bill_list%NOTFOUND;
            begin
              insert into bc_bill_payment_query a (id, contract_no,user_id,service_id,service_code, service_name,amount,description,order_count,status,inq_date) 
              values 
              (SEQ_BILL_PAYMENT_QUERY.NEXTVAL, bill_item.customer_code, 
              bill_item.user_id, bill_item.service_id, bill_item.service_code,bill_item.service_name,0,
              'lay bill dau ngay', bill_item.order_count, 'NEWR', sysdate);
            exception
               WHEN others THEN 
                 rollback;
              dbms_output.put_line('Error!');  
            end;
       
        END LOOP;
       CLOSE bill_list;  
       commit;
       -- save in bk_sys_parameter
       start_record := to_char(end_record);
       update bk_sys_parameter a set
       a.name = start_record
       where 
       a.type = 'pagation_bill' and a.code = 'FR';
       if row_count <= end_record then 
         begin
           update bk_sys_parameter a set
           a.name = to_char(sysdate, 'yyyyMMdd')
           where 
           a.type = 'pagation_bill' and a.code = 'RD';
           
           start_record := to_char(end_record);
           update bk_sys_parameter a set
           a.name = '0'
           where 
           a.type = 'pagation_bill' and a.code = 'FR';
           
         end;
       end if;

       commit;
   else
      DBMS_OUTPUT.PUT_LINE( 'end job!');   
   end if;
  
  end;
end BILL_PAYMENT_BEGIN_DAY;

/
