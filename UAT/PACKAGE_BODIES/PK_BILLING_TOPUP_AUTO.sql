--------------------------------------------------------
--  DDL for Package Body PK_BILLING_TOPUP_AUTO
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."PK_BILLING_TOPUP_AUTO" AS

  PROCEDURE HANDLE_BILLING_TOPUP_AUTO
  (
    P_ACCOUNT_NO VARCHAR2,
    P_PROVIDER VARCHAR2,
    P_PHONE_NUMBER VARCHAR2,
    P_POSTPAID_PHONE_NUMBER VARCHAR2,
    P_AMOUNT NUMBER
  ) 
  AS
    V_USER_ID NUMBER;
    V_SCHDULE_ID NUMBER;
    V_SERVICE_ID NUMBER;
    V_SERVICE_NAME VARCHAR2(100);
    V_SERVICE_CODE VARCHAR2(20);
    V_SERVICE_TYPE VARCHAR2(20);
    V_CATEGORY VARCHAR2(10);
    V_PHONE_NUMBER VARCHAR2(100);
    V_DATE DATE;
    cCustomerCode SYS_REFCURSOR;
    vCustomerRow BC_BILL_PAYMENT_HISTORY%Rowtype;
  BEGIN
    select b.user_id 
    into V_USER_ID
    from bk_account_info a
    left join bc_user_info b on a.cif_no = b.cif_no
    where a.acct_no = P_ACCOUNT_NO and b.status ='ACTV';
    
    select to_date('30/08/2019 23:01:01', 'dd/MM/yyyy HH24:MI:SS') into V_DATE from dual;
    
    IF P_PHONE_NUMBER IS NOT NULL THEN
      select SERVICE_ID, SERVICE_NAME, SERVICE_CODE, SERVICE_TYPE, CATEGORY 
      INTO V_SERVICE_ID, V_SERVICE_NAME, V_SERVICE_CODE, V_SERVICE_TYPE, V_CATEGORY
      from bc_bill_payment_service 
      where UPPER(SERVICE_NAME) = UPPER(P_PROVIDER)
      and service_id in (27,14,17,601,18,20) and status = 'ACTV';
      
      BEGIN
        OPEN cCustomerCode FOR '
        select *
        from BC_BILL_PAYMENT_HISTORY
        where CUSTOMER_CODE =' || P_PHONE_NUMBER
        || ' and is_schedule = ''Y'' and schdule_id is not null and status = ''NEWR'' and rollout_account_no = ' || P_ACCOUNT_NO || ' and service_id = ' || V_SERVICE_ID;
        
        LOOP            
            FETCH cCustomerCode INTO vCustomerRow; 
            IF cCustomerCode%ROWCOUNT = 1 THEN
              V_PHONE_NUMBER := vCustomerRow.CUSTOMER_CODE;
            END IF;
            EXIT WHEN cCustomerCode%NOTFOUND;
        END LOOP;
        EXCEPTION
              WHEN OTHERS THEN 
              V_PHONE_NUMBER := null;
      END;
      -- DBMS_OUTPUT.put_line('-V_PHONE_NUMBER: ' || V_PHONE_NUMBER);
      IF V_USER_ID IS NOT NULL AND V_SERVICE_ID IS NOT NULL and V_PHONE_NUMBER IS NULL THEN
        select SEQ_SCHDULE_ID.nextval INTO V_SCHDULE_ID from dual;
        
        insert into BK_TRAN_SCHEDULE (SCHDULE_ID, FRQ_TYPE, FRQ_INTERVAL, END_TYPE, FRQ_LIMIT, IS_LACK_STOP, DEBIT_DATE, COMPLETE_COUNT, UPDATE_SCH_TIME)
        values (V_SCHDULE_ID, 'M', 0, 'L', 1, 'Y', 0, 0, V_DATE);
        commit;
        
        INSERT INTO BC_BILL_PAYMENT_HISTORY (TRAN_SN, USER_ID, OH_SIGN, IS_SCHEDULE, SCHDULE_ID, SERVICE_ID, SERVICE_NAME, SERVICE_CODE, SERVICE_TYPE, ROLLOUT_ACCOUNT_NO, MOBILE, AMOUNT, FEE, STATUS, CREATE_BY, CREATE_TIME, CHANNEL_CODE, CUSTOMER_CODE, PAID_BILL_CODE, DISCOUNTED_AMT, TRAN_TIME) 
        VALUES 
        (to_char(V_DATE, 'yyyyMMddHH24MISSSSS') || FN_GET_SEQ_AUTO_BILLING(AUTO_BILLING_TOPUP_SEQ.nextval), V_USER_ID, 'S', 'Y', V_SCHDULE_ID, V_SERVICE_ID, V_SERVICE_NAME, V_SERVICE_CODE, V_SERVICE_TYPE, P_ACCOUNT_NO, P_PHONE_NUMBER, P_AMOUNT, 0, 'NEWR', -1, V_DATE, 'IB', P_PHONE_NUMBER, V_CATEGORY, P_AMOUNT, V_DATE);
      END IF;
    ELSE
      insert into bk_log (n, v) values ('PK_BILLING_TOPUP_AUTO error', 'AccountNo: ' || P_ACCOUNT_NO || ', Provider: ' || P_PROVIDER || ', PhoneNumber: ' || P_PHONE_NUMBER || ', PostpaidPhoneNumber: ' || P_POSTPAID_PHONE_NUMBER || ', Amount: ' || P_AMOUNT);
    END IF;
    
    IF P_POSTPAID_PHONE_NUMBER IS NOT NULL THEN
      select SERVICE_ID, SERVICE_NAME, SERVICE_CODE, PAYMENT_TYPE, CATEGORY 
      INTO V_SERVICE_ID, V_SERVICE_NAME, V_SERVICE_CODE, V_SERVICE_TYPE, V_CATEGORY
      from bc_bill_payment_service 
      where UPPER(SERVICE_NAME) = UPPER(P_PROVIDER)
      and service_id in (100003,100004,100028,200119,200122) and status = 'ACTV';
      
      BEGIN
        OPEN cCustomerCode FOR '
        select *
        from BC_BILL_PAYMENT_HISTORY
        where CUSTOMER_CODE =' || P_POSTPAID_PHONE_NUMBER
        || ' and is_schedule = ''Y'' and schdule_id is not null and status = ''NEWR'' and rollout_account_no = ' || P_ACCOUNT_NO || ' and service_id = ' || V_SERVICE_ID;
        LOOP            
            FETCH cCustomerCode INTO vCustomerRow; 
            IF cCustomerCode%ROWCOUNT = 1 THEN
              V_PHONE_NUMBER := vCustomerRow.CUSTOMER_CODE;
            END IF;
            EXIT WHEN cCustomerCode%NOTFOUND;
        END LOOP;
        EXCEPTION
              WHEN OTHERS THEN 
              V_PHONE_NUMBER := null;
      END;
      
      IF V_USER_ID IS NOT NULL AND V_SERVICE_ID IS NOT NULL and V_PHONE_NUMBER IS NULL THEN
        select SEQ_SCHDULE_ID.nextval INTO V_SCHDULE_ID from dual;
        
        INSERT INTO BC_BILL_PAYMENT_HISTORY (TRAN_SN, USER_ID, OH_SIGN, IS_SCHEDULE, SCHDULE_ID, SERVICE_ID, SERVICE_NAME, SERVICE_CODE, SERVICE_TYPE, ROLLOUT_ACCOUNT_NO, MOBILE, AMOUNT, FEE, STATUS, CREATE_BY, CREATE_TIME, CHANNEL_CODE, CUSTOMER_CODE, PAID_BILL_CODE, DISCOUNTED_AMT, TRAN_TIME) 
        VALUES 
        (to_char(V_DATE, 'yyyyMMddHH24MISSSSS') || FN_GET_SEQ_AUTO_BILLING(AUTO_BILLING_TOPUP_SEQ.nextval), V_USER_ID, 'S', 'Y', V_SCHDULE_ID, V_SERVICE_ID, V_SERVICE_NAME, V_SERVICE_CODE, V_SERVICE_TYPE, P_ACCOUNT_NO, P_POSTPAID_PHONE_NUMBER, 0, 0, 'NEWR', -1, V_DATE, 'IB', P_POSTPAID_PHONE_NUMBER, 0, 0, V_DATE);
      END IF;
    ELSE
      insert into bk_log (n, v) values ('PK_BILLING_TOPUP_AUTO error', 'AccountNo: ' || P_ACCOUNT_NO || ', Provider: ' || P_PROVIDER || ', PhoneNumber: ' || P_PHONE_NUMBER || ', PostpaidPhoneNumber: ' || P_POSTPAID_PHONE_NUMBER || ', Amount: ' || P_AMOUNT);
    END IF;
    
    commit;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
      insert into bk_log (n, v) values ('PK_BILLING_TOPUP_AUTO error', 'AccountNo: ' || P_ACCOUNT_NO || ', Provider: ' || P_PROVIDER || ', PhoneNumber: ' || P_PHONE_NUMBER || ', PostpaidPhoneNumber: ' || P_POSTPAID_PHONE_NUMBER || ', Amount: ' || P_AMOUNT);
  END;
END PK_BILLING_TOPUP_AUTO;

/
