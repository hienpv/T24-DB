--------------------------------------------------------
--  DDL for Package Body PK_DISCOUNT_PROCESS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."PK_DISCOUNT_PROCESS" AS
  PROCEDURE PROCESS_DATA_DISCOUNT
  AS
    V_START_TIME DATE;
    V_QUANTITY_USED NUMBER; -- So luong da su dung
    V_QUANTITY_REMAIN NUMBER; -- So luong con lai
    V_MAX_COUNT NUMBER;
  BEGIN
    FOR discount_rec IN (
      select * from bk_discount_config 
      where status = 'ACTV' and start_time <= sysdate and (end_time >= sysdate or end_time is null) and (max_no is null or max_no > count)  
    )
    LOOP
      select (discount_rec.start_time + discount_rec.number_day*discount_rec.count) into V_START_TIME from dual;
      IF trunc(V_START_TIME) = trunc(sysdate) and (discount_rec.MAX_NO IS NULL or discount_rec.COUNT < discount_rec.MAX_NO) THEN
        update bk_discount_service 
        set val_time = V_START_TIME
        where code = discount_rec.code;
        
--        IF discount_rec.COUNT = 0 THEN
--          -- Update so luong ma khuyen mai con lai
--          select count(*) INTO V_QUANTITY_USED
--          from bc_bill_payment_history 
--          where service_type='QR' and wf_process_id = discount_rec.code
--          and service_name = 'QRCODE'
--          and trunc(create_time) <= trunc(sysdate - 1)
--          and status not in ('FAIL','DLTD','REGS');
--          
--          select nvl(max_count, 0) into V_MAX_COUNT from BK_DISCOUNT_SERVICE where code = discount_rec.code and status = 1;
--          
--          V_QUANTITY_REMAIN := (V_MAX_COUNT - V_QUANTITY_USED);
--          
--          update BK_DISCOUNT_SERVICE
--          -- set max_count = (case when (V_QUANTITY_REMAIN > 0) then V_QUANTITY_REMAIN else 0 end) + nvl(discount_rec.NUMBER_CODE_PERIOD, 0)
--          set max_count = nvl(discount_rec.NUMBER_CODE_PERIOD, 0)
--          where code = discount_rec.code and status = 1;
--        ELSE
--          -- Update so luong ma khuyen mai con lai
--          select count(*) INTO V_QUANTITY_USED
--          from bc_bill_payment_history 
--          where service_type='QR' and wf_process_id = discount_rec.code
--          and service_name = 'QRCODE'
--          and trunc(create_time) <= trunc(sysdate - 1) and trunc(create_time) >= (discount_rec.start_time + discount_rec.number_day*(discount_rec.count-1))
--          and status not in ('FAIL','DLTD','REGS');
--          
--          select nvl(max_count, 0) into V_MAX_COUNT from BK_DISCOUNT_SERVICE where code = discount_rec.code and status = 1;
--          
--          V_QUANTITY_REMAIN := (V_MAX_COUNT - V_QUANTITY_USED);
--          
--          update BK_DISCOUNT_SERVICE
--          -- set max_count = (case when (V_QUANTITY_REMAIN > 0) then V_QUANTITY_REMAIN else 0 end) + nvl(discount_rec.NUMBER_CODE_PERIOD, 0)
--          set max_count = nvl(discount_rec.NUMBER_CODE_PERIOD, 0)
--          where code = discount_rec.code and status = 1;
--        END IF;
        
        
        -- Update so luong chay config
        update bk_discount_config
        set count = count + 1
        where code = discount_rec.code;
        
      END IF;
      
    END LOOP;
    commit;
  END;
END PK_DISCOUNT_PROCESS;

/
