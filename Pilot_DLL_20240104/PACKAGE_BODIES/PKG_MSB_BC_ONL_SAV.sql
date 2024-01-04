--------------------------------------------------------
--  DDL for Package Body PKG_MSB_BC_ONL_SAV
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."PKG_MSB_BC_ONL_SAV" 
IS
   FUNCTION FN_GET_INR_SAV(CATEGORY IN VARCHAR2,
     CURRENTCY IN VARCHAR2,
     AMOUNT IN VARCHAR2,
     LANGUAGE IN VARCHAR2,
     CHANNEL_CODE IN VARCHAR2
     ) RETURN NUMBER
    IS
          l_result NUMBER;            
          l_sql VARCHAR2(4000);
          
          v_category VARCHAR2(100);
          v_currency VARCHAR2(100);
          v_amount VARCHAR2(100);
          v_language VARCHAR2(100);
          v_channel_code VARCHAR2(100);
          
          CURSOR map_bal_inr_cursor IS 
          select product.BALRT as BALRT, rate.JRCRAT as rate from 
          (select
              t.PTYPE,
              t.pdesc,
              case cj.product_code
                  when  PKG_MSB_BC_ONL_SAV.PRATEN  then -1
                  when  PKG_MSB_BC_ONL_SAV.BALRT2  then t.BALRT2
                  when  PKG_MSB_BC_ONL_SAV.BALRT3  then t.BALRT3
                  when  PKG_MSB_BC_ONL_SAV.BALRT4  then t.BALRT4
                  when  PKG_MSB_BC_ONL_SAV.BALRT5  then t.BALRT5
                  when  PKG_MSB_BC_ONL_SAV.BALRT6  then t.BALRT6
                  when  PKG_MSB_BC_ONL_SAV.BALRT7  then t.BALRT7
                  when  PKG_MSB_BC_ONL_SAV.BALRT8  then t.BALRT8
                  when  PKG_MSB_BC_ONL_SAV.BALRT9  then t.BALRT9       
                  when  PKG_MSB_BC_ONL_SAV.BALRT0  then t.BALRT0
                  when  PKG_MSB_BC_ONL_SAV.BALRTA  then t.BALRTA
                  when  PKG_MSB_BC_ONL_SAV.BALRTB  then t.BALRTB
              end as BALRT
              , case cj.product_code
                  when  PKG_MSB_BC_ONL_SAV.PRATEN  then t.PRATEN
                  when  PKG_MSB_BC_ONL_SAV.BALRT2  then t.INTRT2
                  when  PKG_MSB_BC_ONL_SAV.BALRT3  then t.INTRT3
                  when  PKG_MSB_BC_ONL_SAV.BALRT4  then t.INTRT4
                  when  PKG_MSB_BC_ONL_SAV.BALRT5  then t.INTRT5
                  when  PKG_MSB_BC_ONL_SAV.BALRT6  then t.INTRT6
                  when  PKG_MSB_BC_ONL_SAV.BALRT7  then t.INTRT7
                  when  PKG_MSB_BC_ONL_SAV.BALRT8  then t.INTRT8
                  when  PKG_MSB_BC_ONL_SAV.BALRT9  then t.INTRT9
                  when  PKG_MSB_BC_ONL_SAV.BALRT0  then t.INTRT0
                  when  PKG_MSB_BC_ONL_SAV.BALRTA  then t.INTRTA
                  when  PKG_MSB_BC_ONL_SAV.BALRTB  then t.INTRTB        
              end as INTRT
              , case cj.product_code 
                  when  PKG_MSB_BC_ONL_SAV.PRATEN  then 1
                  when  PKG_MSB_BC_ONL_SAV.BALRT2  then 2
                  when  PKG_MSB_BC_ONL_SAV.BALRT3  then 3
                  when  PKG_MSB_BC_ONL_SAV.BALRT4  then 4
                  when  PKG_MSB_BC_ONL_SAV.BALRT5  then 5
                  when  PKG_MSB_BC_ONL_SAV.BALRT6  then 6
                  when  PKG_MSB_BC_ONL_SAV.BALRT7  then 7
                  when  PKG_MSB_BC_ONL_SAV.BALRT8  then 8
                  when  PKG_MSB_BC_ONL_SAV.BALRT9  then 9
                  when  PKG_MSB_BC_ONL_SAV.BALRT0  then 10
                  when  PKG_MSB_BC_ONL_SAV.BALRTA  then 11
                  when  PKG_MSB_BC_ONL_SAV.BALRTB  then 12        
              end as SEQ
          from SYNC_CDPAR2 t 
          cross join ( 
              select  PKG_MSB_BC_ONL_SAV.PRATEN  as product_code from dual union all
              select  PKG_MSB_BC_ONL_SAV.BALRT2  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT3  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT4  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT5  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT6  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT7  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT8  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT9  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRT0  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRTA  from dual union all 
              select  PKG_MSB_BC_ONL_SAV.BALRTB  from dual 
              ) cj
          where TRIM(t.PTYPE) in (v_category) and TRIM(t.pcurty) = v_currency ) product
      left join sync_ssrate rate on rate.jrratn = product.INTRT 
          where 1=1
            and TRIM(rate.jrrcur) = v_currency
            --and TO_NUMBER(product.balrt) >= TO_NUMBER(v_amount)
            --and rownum = 1
          order by product.BALRT desc;
    BEGIN
        l_result := PKG_MSB_BC_ONL_SAV.RESULT_EXCP; 
        
        v_category := CATEGORY;
        v_currency := CURRENTCY;
        v_language := LANGUAGE;
        -- v_amount := RPAD(AMOUNT, LENGTH(AMOUNT) + 2 , '0');
        v_amount := AMOUNT;
        v_channel_code := CHANNEL_CODE;
        DBMS_OUTPUT.put_line(v_amount);
        BEGIN
            FOR map_bal_inr_rec IN map_bal_inr_cursor LOOP 
                IF v_amount >= map_bal_inr_rec.BALRT 
                    AND map_bal_inr_rec.rate IS NOT NULL THEN
                        l_result := map_bal_inr_rec.rate;
                    EXIT;
                END IF;                 
            END LOOP;        
          DBMS_OUTPUT.put_line(l_result); 
        EXCEPTION            
         WHEN NO_DATA_FOUND THEN
            DBMS_OUTPUT.put_line('step1');
            l_result := PKG_MSB_BC_ONL_SAV.RESULT_EXCP;    --Do nothing
        END;
          
           
        /*
          EXECUTE IMMEDIATE
            l_sql
            INTO l_result
          USING CATEGORY,CURRENTCY,AMOUNT;    */
          
          /*
          
          IF l_result IS NULL OR l_result = PKG_MSB_BC_ONL_SAV.RESULT_EXCP THEN   
          DBMS_OUTPUT.put_line('step2');   
            BEGIN
                select rate.JRCRAT INTO l_result from SYNC_CDPAR2 cdp
                            join sync_ssrate rate on rate.jrratn = cdp.PRATEN 
                            where 1=1
                              and TRIM(cdp.PTYPE) = v_category
                              and TRIM(cdp.pcurty) = v_currency 
                              and TRIM(rate.jrrcur) = v_currency;
                 DBMS_OUTPUT.put_line(l_result);                               
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.put_line('step4');       
                l_result := PKG_MSB_BC_ONL_SAV.RESULT_EXCP;    --Do nothing
            END; 
          END IF;
          
          
          IF l_result IS NULL OR l_result = PKG_MSB_BC_ONL_SAV.RESULT_EXCP THEN  
            BEGIN
                select rp.rate INTO l_result from bk_receipt_product rp 
                  where rp.sys_code = v_channel_code
                  and rp.product_code = v_category;  
            EXCEPTION
            WHEN NO_DATA_FOUND THEN
                DBMS_OUTPUT.put_line('step5');
                l_result := PKG_MSB_BC_ONL_SAV.RESULT_EXCP;    --Do nothing
            END; 
          END IF;          
         */
          
        RETURN l_result;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line(SQLERRM);           
            RETURN PKG_MSB_BC_ONL_SAV.RESULT_EXCP;
    END;
    
END PKG_MSB_BC_ONL_SAV;

/
