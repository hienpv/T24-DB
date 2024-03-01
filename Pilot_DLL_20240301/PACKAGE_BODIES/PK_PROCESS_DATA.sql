--------------------------------------------------------
--  DDL for Package Body PK_PROCESS_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_PROCESS_DATA" AS


  PROCEDURE UPDATE_CUSTOMER_MOBILE_PAYMENT
  AS
  BEGIN
--    FOR transfer_rec IN (
--      select tran_sn, customer_code, FN_CONVERT_MOBILE_TEN_NO (customer_code) customer_code_new 
--      from bc_bill_payment_history 
--      where IS_SCHEDULE = 'Y' and is_number(customer_code) > 0 and length(customer_code) > 10 and status ='NEWR'
--    )
--    LOOP
--      update bc_bill_payment_history 
--			set 
--				customer_code = transfer_rec.customer_code_new
--        where tran_sn=transfer_rec.tran_sn;
--    END LOOP;
--    commit;
    null;
  END;
  
  PROCEDURE UPDATE_MOBILE_PREPAID
  AS
  BEGIN
--    FOR transfer_rec IN (
--      select FN_CONVERT_MOBILE_TEN_NO(bui.mobile) mobile_new, bui.mobile, bui.user_id
--      from bc_card_info bci
--      left join bc_user_info bui on bui.user_id = bci.user_id
--      where length(bui.mobile) > 10
--    )
--    LOOP
--      update bc_user_info 
--			set 
--				mobile = transfer_rec.mobile_new
--      where user_id=transfer_rec.user_id;
--      commit;
--    END LOOP;
    null;
  END;
  
  PROCEDURE UPDATE_BENEFIT_TRANSFER
  AS
  BEGIN
--    FOR transfer_rec IN (
--      select bth.beneficiary_account_no as benefit_account, bth.beneficiary_name as benefit_name, bth.beneficiary_bank_name as benefit_bank,
--      bth.rollout_account_no as rollout_account, bth.beneficiary_branch_name, bth.core_sn
--      from bc_transfer_history bth
--      left join bk_account_history bah on bah.rollout_acct_no = bth.rollout_account_no and bah.tm_seq=bth.core_sn
--      where bth.is_inter_bank = 'Y'
--      and bth.status = 'SUCC'
--      and bth.create_time > (sysdate - (1/1440*10))
--      and beneficiary_acct_no is null
--    )
--    LOOP
--      update bk_account_history 
--			set 
--				beneficiary_acct_no = transfer_rec.benefit_account,
--				beneficiary_acct_name = transfer_rec.benefit_name,
--				beneficiary_acct_bank = transfer_rec.benefit_bank,
--				beneficiary_acct_branch = transfer_rec.beneficiary_branch_name
--        where rollout_acct_no=transfer_rec.rollout_account
--        and tm_seq=transfer_rec.core_sn
--        and tran_time > trunc(sysdate);
--      commit;
--    END LOOP;
    null;
  END;

  PROCEDURE UPDATE_EMAIL_USER_INFO
  AS
    V_HOUR NUMBER;
  BEGIN
--    select to_number(to_char(sysdate, 'HH24')) 
--    into V_HOUR
--    from dual;
--    IF V_HOUR >= 8 and V_HOUR <= 18 THEN
--      update bc_user_info set email=null where length(email)=30 and ascii(email)=0;
--      commit;
--    END IF;
    null;
  END;

  PROCEDURE UPDATE_RELATED_ACCOUNT
  AS
  BEGIN
--    update bc_related_account 
--    set status = 'DLTD', update_by=-1, update_time=sysdate  
--    where acct_no in (select acct_no from bk_account_info where product_type = 'R-CADKSH') and status='ACTV';
--    commit;
    null;
  END;

  PROCEDURE CONVERT_MOBILE_BC_USER_INFO
  AS
  BEGIN
--    FOR mobile_rec IN (
--      select bui.user_id, bui.mobile, mpt.mobilephone_new
--      from bc_user_info bui
--      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.mobile) = mpt.mobilephone
--      where bui.mobile is not null and mpt.mobilephone is not null
--    )
--    LOOP
--      update bc_user_info 
--      set mobile = ('0' || SUBSTR(mobile_rec.mobilephone_new,3, LENGTH(mobile_rec.mobilephone_new)))
--      where user_id = mobile_rec.user_id;
--      commit;
--    END LOOP;
--
--    FOR telephone_rec IN (
--      select bui.user_id, bui.telephone, mpt.mobilephone_new
--      from bc_user_info bui
--      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.telephone) = mpt.mobilephone
--      where bui.telephone is not null and mpt.mobilephone is not null
--    )
--    LOOP
--      update bc_user_info 
--      set telephone = ('0' || SUBSTR(telephone_rec.mobilephone_new,3, LENGTH(telephone_rec.mobilephone_new))) 
--      where user_id = telephone_rec.user_id;
--      commit;
--    END LOOP;
--    
--    FOR mobile_mbs_rec IN (
--      select bui.user_id, bui.mobile_mbs, mpt.mobilephone_new
--      from bc_user_info bui
--      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.mobile_mbs) = mpt.mobilephone
--      where bui.mobile_mbs is not null and mpt.mobilephone is not null
--    )
--    LOOP
--      update bc_user_info 
--      set mobile_mbs = ('0' || SUBSTR(mobile_mbs_rec.mobilephone_new,3, LENGTH(mobile_mbs_rec.mobilephone_new))) 
--      where user_id = mobile_mbs_rec.user_id;
--      commit;
--    END LOOP;
--
--    FOR mobile_sms_rec IN (
--      select bui.user_id, bui.mobile_sms, mpt.mobilephone_new
--      from bc_user_info bui
--      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.mobile_sms) = mpt.mobilephone
--      where bui.mobile_sms is not null and mpt.mobilephone is not null
--    )
--    LOOP
--      update bc_user_info 
--      set mobile_sms = ('0' || SUBSTR(mobile_sms_rec.mobilephone_new,3, LENGTH(mobile_sms_rec.mobilephone_new)))
--      where user_id = mobile_sms_rec.user_id;
--      commit;
--    END LOOP;
    null;
  END;

  PROCEDURE CONVERT_MOBILE_BB_USER_INFO
  AS
  BEGIN
    FOR mobile_rec IN (
      select bui.user_id, bui.mobile, mpt.mobilephone_new
      from bb_user_info bui
      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.mobile) = mpt.mobilephone
      where bui.mobile is not null and mpt.mobilephone is not null
    )
    LOOP
      update bb_user_info 
      set mobile = ('0' || SUBSTR(mobile_rec.mobilephone_new,3, LENGTH(mobile_rec.mobilephone_new))) 
      where user_id = mobile_rec.user_id;
      commit;
    END LOOP;

    FOR telephone_rec IN (
      select bui.user_id, bui.telephone, mpt.mobilephone_new
      from bb_user_info bui
      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.telephone) = mpt.mobilephone
      where bui.telephone is not null and mpt.mobilephone is not null
    )
    LOOP
      update bb_user_info 
      set telephone = ('0' || SUBSTR(telephone_rec.mobilephone_new,3, LENGTH(telephone_rec.mobilephone_new)))
      where user_id = telephone_rec.user_id;
      commit;
    END LOOP;

    FOR mobile_mbs_rec IN (
      select bui.user_id, bui.mobile_mbs, mpt.mobilephone_new
      from bb_user_info bui
      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.mobile_mbs) = mpt.mobilephone
      where bui.mobile_mbs is not null and mpt.mobilephone is not null
    )
    LOOP
      update bb_user_info 
      set mobile_mbs = ('0' || SUBSTR(mobile_mbs_rec.mobilephone_new,3, LENGTH(mobile_mbs_rec.mobilephone_new))) 
      where user_id = mobile_mbs_rec.user_id;
      commit;
    END LOOP;

    FOR mobile_sms_rec IN (
      select bui.user_id, bui.mobile_sms, mpt.mobilephone_new
      from bb_user_info bui
      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(bui.mobile_sms) = mpt.mobilephone
      where bui.mobile_sms is not null and mpt.mobilephone is not null
    )
    LOOP
      update bb_user_info 
      set mobile_mbs = ('0' || SUBSTR(mobile_sms_rec.mobilephone_new,3, LENGTH(mobile_sms_rec.mobilephone_new)))
      where user_id = mobile_sms_rec.user_id;
      commit;
    END LOOP;
  END;

  PROCEDURE CONVERT_MOBILE_BILL_PAYMENT
  AS
  BEGIN
    -- Nap tien
--    FOR mobile_rec IN (
--      select a.tran_sn, mpt.mobilephone_new from (
--      select * 
--      from bc_bill_payment_history 
--      where is_schedule = 'Y' and status = 'NEWR' and service_id in (27,14,17,601,18,20)
--      order by create_time desc
--      ) a
--      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(a.customer_code) = mpt.mobilephone
--      where a.customer_code is not null and mpt.mobilephone is not null
--    )
--    LOOP
--      update bc_bill_payment_history
--      set 
--        MOBILE = ('0' || SUBSTR(mobile_rec.mobilephone_new,3, LENGTH(mobile_rec.mobilephone_new))),
--        CUSTOMER_CODE = ('0' || SUBSTR(mobile_rec.mobilephone_new,3, LENGTH(mobile_rec.mobilephone_new)))
--      where tran_sn = mobile_rec.tran_sn;
--      commit;
--    END LOOP;
--
--    -- Thanh toan hoa don
--    FOR bill_payment_rec IN (
--      select a.tran_sn, mpt.mobilephone_new from (
--      select * 
--      from bc_bill_payment_history 
--      where is_schedule = 'Y' and status = 'NEWR' and service_id in (100003,100004,100028,200119,200122)
--      order by create_time desc
--      ) a
--      left join migrate_phone_tbl mpt on FN_GET_CONVERT_MOBILE(a.customer_code) = mpt.mobilephone
--      where a.customer_code is not null and mpt.mobilephone is not null
--    )
--    LOOP
--      update bc_bill_payment_history
--      set 
--        MOBILE = ('0' || SUBSTR(bill_payment_rec.mobilephone_new,3, LENGTH(bill_payment_rec.mobilephone_new))),
--        CONTRACT_NO = ('0' || SUBSTR(bill_payment_rec.mobilephone_new,3, LENGTH(bill_payment_rec.mobilephone_new))),
--        CUSTOMER_CODE = ('0' || SUBSTR(bill_payment_rec.mobilephone_new,3, LENGTH(bill_payment_rec.mobilephone_new))),
--        PAID_BILL_CODE = ('0' || SUBSTR(bill_payment_rec.mobilephone_new,3, LENGTH(bill_payment_rec.mobilephone_new)))
--      where tran_sn = bill_payment_rec.tran_sn;
--      commit;
--    END LOOP;
    null;
  END;
  
  PROCEDURE CONVERT_DATA_WORKFLOW_ACTIVITI
  AS
    V_PROCESS_LEVEL NUMBER;
    V_PROCESS_ID NUMBER;
  BEGIN
--    FOR transfer_rec IN (
--      select * from wf_bb_process wbp 
--      where wbp.is_limit = 1 and wbp.status = 'DLTD' and min_amount is null and max_amount is null
--      -- and corp_id=1437495 and process_id=16341
--    )
--    LOOP
--    
--      FOR Lcntr IN 1..transfer_rec.process_level
--      LOOP
--      
--         FOR assignee_rec IN (
--          select * from wf_bb_assignee 
--          where corp_id = transfer_rec.corp_id and PROCESS_DEFINITION_KEY = transfer_rec.PROCESS_DEFINITION_KEY and process_level <= Lcntr
--          and trunc(create_time) < trunc(sysdate)
--         )
--         LOOP
--             select process_id into V_PROCESS_ID from wf_bb_process 
--             where process_definition_key = transfer_rec.process_definition_key and corp_id = transfer_rec.corp_id and status = 'ACTV' and process_level = Lcntr;
--         
--            INSERT INTO WF_BB_ASSIGNEE(
--              ASSIGNEE_ID,PROCESS_ID,PROCESS_LEVEL,CORP_ID,USER_ID,USER_NAME,CREATE_BY,CREATE_TIME,PROCESS_DEFINITION_KEY)
--            values(SEQ_ASSIGNEE_ID.nextval,V_PROCESS_ID,assignee_rec.process_level,transfer_rec.corp_id,
--              assignee_rec.user_id,assignee_rec.user_name,assignee_rec.create_By,sysdate,assignee_rec.PROCESS_DEFINITION_KEY);
--            
--         END LOOP;
--      END LOOP;
--      
--    END LOOP;
--    commit;
    null;
  END;
  
  PROCEDURE CALCULATE_PROMOTION_EVERYDAY
  AS
    V_QUANTITY_USED NUMBER; -- So luong da su dung
    V_QUANTITY_REMAIN NUMBER; -- So luong con lai
  BEGIN
--    FOR discount_service_rec IN (
--      select code, max_count 
--      from BK_DISCOUNT_SERVICE where code in (
--        select name from bk_sys_parameter where type='PROMOTION_EVERYDAY' and code='PROMOTION_CODE'
--      ) and status = 1
--    )
--    LOOP
--      select count(*) INTO V_QUANTITY_USED
--      from bc_bill_payment_history 
--      where service_type='QR' and wf_process_id = discount_service_rec.code
--      and service_name = 'QRCODE'
--      and tran_sn like ('' || to_char(sysdate-1, 'yyyymmdd') || '%')
--      and status not in ('FAIL','DLTD','REGS');
--      
--      V_QUANTITY_REMAIN := (discount_service_rec.max_count - V_QUANTITY_USED);
--      
--      update BK_DISCOUNT_SERVICE
--      set val_time = trunc(sysdate),
--          max_count = (case when (V_QUANTITY_REMAIN > 0) then V_QUANTITY_REMAIN else 0 end)
--      where code = discount_service_rec.code and status = 1;
--      
--    END LOOP;
--    commit;
    null;
  END;
  
  PROCEDURE UPDATE_DEFAULT_WORKFLOW
  AS
  BEGIN
--    update wf_bb_process
--    set is_limit = 0,
--    min_amount = 0,
--    max_amount = 99999999999999999999,
--    UPDATE_BY = -1,
--    UPDATE_TIME = sysdate
--    where is_limit is null and min_amount is null and max_amount is null;
--    commit;
    null;
  END;
  
  PROCEDURE UPDATE_ACCT_TYPE_RELATED
  AS
  BEGIN
--    FOR acct_rec IN (
--      select bai.acct_type acct_type_source, bra.* 
--      from BC_RELATED_ACCOUNT bra
--      left join bk_account_info bai on bra.acct_no= bai.acct_no
--      where bra.acct_type is null
--    )
--    LOOP
--      update BC_RELATED_ACCOUNT
--      set ACCT_TYPE = acct_rec.acct_type_source,
--        update_by = -1,
--        update_time = sysdate
--      where relation_id = acct_rec.relation_id;
--    END LOOP;
--    commit;
    null;
  END; 
  
  PROCEDURE CALCULATE_CIF_NO_FROM_CORE
  AS
  BEGIN
--    FOR user_rec IN (
--      select a.user_id, a.cert_code, a.cert_type, a.cif_no, trim(b.CFCIFN) CFCIFN, trim(b.CFSSNO) cert_code_new, trim(b.CFSSCD) cert_type_new
--      from bc_user_info a
--      left join svdatpv51.cfmast@DBLINK_DATA b on to_number(a.cif_no) = trim(b.CFCIFN)
--      where a.status = 'ACTV' and length(a.cert_code) < 9 and a.cert_code != trim(b.CFSSNO) and rownum <= 10000
--    )
--    LOOP
--      update bc_user_info
--      set cert_code = user_rec.cert_code_new,
--        cert_type = user_rec.cert_type_new,
--        update_by = -1,
--        update_time = sysdate
--      where user_id = user_rec.user_id;
--    END LOOP;
--    commit;
    null;
  END;
  
  PROCEDURE UPDATE_CIF_NO_FROM_CORE
  AS
  BEGIN
--    FOR user_rec IN (
--      select a.user_id, a.cert_code, a.cert_type, a.cif_no, trim(b.CFCIFN) CFCIFN, trim(b.CFSSNO) cert_code_new, trim(b.CFSSCD) cert_type_new
--      from bc_user_info a
--      left join svdatpv51.cfmast@DBLINK_DATA b on to_number(a.cif_no) = trim(b.CFCIFN)
--      where a.cif_no in (select cif_no from test_cif)
--    )
--    LOOP
--      update bc_user_info
--      set cert_code = user_rec.cert_code_new,
--        cert_type = user_rec.cert_type_new,
--        update_by = -1,
--        update_time = sysdate
--      where user_id = user_rec.user_id;
--    END LOOP;
--    commit;
    null;
  END;
  
  PROCEDURE PROCESS_CAPTCHA
  AS
  BEGIN
    delete from bb_captcha a where create_time < sysdate -10/60/24 and rownum <= 100000;
    commit;
  END;
  
  PROCEDURE WORD_GAME_SET_SEQUENCE (p_seq_name in varchar2)
  AS
    l_val number;
  BEGIN
    execute immediate 'select ' || p_seq_name || '.nextval from dual' INTO l_val;
    execute immediate 'alter sequence ' || p_seq_name || ' increment by -' || l_val || ' minvalue 0';
    execute immediate 'select ' || p_seq_name || '.nextval from dual' INTO l_val;
    execute immediate 'alter sequence ' || p_seq_name || ' increment by 1 minvalue 0';
    commit;
  END;
  
  PROCEDURE SYNC_DATA_BK_CIF
  AS
  BEGIN
    -- 136174 record
--    FOR user_rec IN (
--        select trim(d.CFCIFN) CIF_NO, trim(d.CFSSCD) CERT_TYPE, trim(d.CFSSNO) CERT_CODE, '302' BANK_NO, 
--        CASE WHEN LENGTH(trim(d.cfbrnn)) = 2 then ('0' || trim(d.cfbrnn)) else trim(d.cfbrnn) end ORG_NO, 
--        trim(d.cfna1) CIF_ACCT_NAME, TO_DATE(d.cfbird,'yyyyddd') BIRTH_DATE, trim(d.CFBIRP) BIRTH_PLACE, trim(d.CFCITZ) COUNTRY, trim(d.CFINDI) INDIVIDUAL,
--        c.TELEPHONE TELEPHONE, c.MOBILE MOBILE, c.ADDRESS ADDR, c.POSTAL_CODE POSTAL_CODE, c.EMAIL EMAIL, 0 as SYNC_HIST, 0 as ID, trim(d.taxcod) TAXCOD
--        from (
--          select a.* from (
--            select cif_no, TELEPHONE, MOBILE, ADDRESS, POSTAL_CODE, EMAIL  
--            from bc_user_info
--            where status = 'ACTV'
--          ) a
--          left join bk_cif b on a.cif_no = b.cif_no
--          where b.cif_no is null and a.cif_no in (select cif_no from bk_cif_sync where run_time is null)
--        ) c
--        left join svdatpv51.cfmast@DBLINK_DATA d on to_number(c.cif_no) = trim(d.CFCIFN)
--        where c.cif_no is not null and d.CFCIFN is not null
--      )
--      LOOP
--        insert into BK_CIF (CIF_NO, CERT_TYPE, CERT_CODE, BANK_NO, ORG_NO, CIF_ACCT_NAME, BIRTH_DATE, BIRTH_PLACE, COUNTRY, INDIVIDUAL,
--        TELEPHONE, MOBILE, ADDR, POSTAL_CODE, EMAIL, SYNC_HIST, ID, TAXCOD)
--        values (user_rec.CIF_NO, user_rec.CERT_TYPE, user_rec.CERT_CODE, user_rec.BANK_NO, user_rec.ORG_NO, user_rec.CIF_ACCT_NAME, user_rec.BIRTH_DATE, user_rec.BIRTH_PLACE, user_rec.COUNTRY, user_rec.INDIVIDUAL,
--        user_rec.TELEPHONE, user_rec.MOBILE, user_rec.ADDR, user_rec.POSTAL_CODE, user_rec.EMAIL, user_rec.SYNC_HIST, user_rec.ID, user_rec.TAXCOD);
--      END LOOP;
--      commit;
    null;
  END;
  
  PROCEDURE SYNC_DATA_BK_CIF_CORP
  AS
  BEGIN
    -- 136174 record
    FOR user_rec IN (
      select trim(d.CFCIFN) CIF_NO, trim(d.CFSSCD) CERT_TYPE, nvl(trim(d.CFSSNO), null) CERT_CODE, '302' BANK_NO, 
      CASE WHEN LENGTH(trim(d.cfbrnn)) = 2 then ('0' || trim(d.cfbrnn)) else trim(d.cfbrnn) end ORG_NO, 
      trim(d.cfna1) CIF_ACCT_NAME, TO_DATE(d.cfbird,'yyyyddd') BIRTH_DATE, trim(d.CFBIRP) BIRTH_PLACE, trim(d.CFCITZ) COUNTRY, trim(d.CFINDI) INDIVIDUAL,
      c.TELEPHONE TELEPHONE, c.MOBILE MOBILE, c.ADDRESS ADDR, c.POSTAL_CODE POSTAL_CODE, c.EMAIL EMAIL, 0 as SYNC_HIST, 0 as ID, trim(d.taxcod) TAXCOD
      from (
        select a.* from (
          select cif_no, TELEPHONE, MOBILE, ADDRESS, POSTAL_CODE, EMAIL  
          from bb_corp_info
          where status = 'ACTV'
        ) a
        left join bk_cif b on a.cif_no = b.cif_no
        where b.cif_no is null and a.cif_no in (select cif_no from bk_cif_sync where run_time is null)
      ) c
      left join svdatpv51.cfmast@DBLINK_DATA d on to_number(c.cif_no) = trim(d.CFCIFN)
      where c.cif_no is not null and d.CFCIFN is not null
    )
    LOOP
      insert into BK_CIF (CIF_NO, CERT_TYPE, CERT_CODE, BANK_NO, ORG_NO, CIF_ACCT_NAME, BIRTH_DATE, BIRTH_PLACE, COUNTRY, INDIVIDUAL,
      TELEPHONE, MOBILE, ADDR, POSTAL_CODE, EMAIL, SYNC_HIST, ID, TAXCOD)
      values (user_rec.CIF_NO, user_rec.CERT_TYPE, user_rec.CERT_CODE, user_rec.BANK_NO, user_rec.ORG_NO, user_rec.CIF_ACCT_NAME, user_rec.BIRTH_DATE, user_rec.BIRTH_PLACE, user_rec.COUNTRY, user_rec.INDIVIDUAL,
      user_rec.TELEPHONE, user_rec.MOBILE, user_rec.ADDR, user_rec.POSTAL_CODE, user_rec.EMAIL, user_rec.SYNC_HIST, user_rec.ID, user_rec.TAXCOD);
    
      update bk_cif_sync set run_time=sysdate where cif_no=user_rec.CIF_NO;
    END LOOP;
    commit;
  END;
END PK_PROCESS_DATA;

/
