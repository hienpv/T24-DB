--------------------------------------------------------
--  DDL for Package Body KEEP_DATA_HIS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "KEEP_DATA_HIS" AS


    function fn_partition_is_earlier(i_part_tab_name varchar2, i_partition_position number, i_ref_date in date) 
    return number 
    is
        l_date_str varchar2(2000);
        l_date date;
    begin
        execute immediate 'select high_value from all_tab_partitions where table_name = :tab and partition_position = :pos'
            into l_date_str
            using i_part_tab_name, i_partition_position;
    
        execute immediate 'select ' || l_date_str || ' from dual' into l_date;
        if (l_date < i_ref_date) then
            return 1;
        end if;
        return 0;
    end fn_partition_is_earlier;

  PROCEDURE keep_his_bc_user_info AS
    v_storeDays number(3,0);
    v_InsertHisQueyr varchar2(3000);
    v_dropParDate varchar2(20);
  BEGIN
      select CODE into v_storeDays from bk_sys_parameter where TYPE  =  'BC_USER_INFO_STORAGE';
      select to_char(trunc(sysdate) - v_storeDays, 'YYYYMMDD') into v_dropParDate from dual;
      dbms_output.put_line('dau tien la luu tru du lieu ngay hien tai');
      v_InsertHisQueyr := 'INSERT INTO bc_user_info_his (
            POSTED_DT,     user_id,    user_name,
                nick,    gender,    group_id,
                trade_pwd,    security_type,    sign_org,
                cert_type,    cert_name,    cert_code,
                cert_issued_date,    cert_issued_place,    telephone,
                mobile,    fax,    address,    postal_code,
                email,    open_acct_stmt,    acct_stmt_frq,    acct_stmt_method,
                open_mobile,    service_type,    receive_sms_adv,
                remark,    create_by,    create_by_mng,    create_time,    update_by,
                update_by_mng,    update_time,    status,    freezed_start_time,
                freezed_end_time,    login_count,    is_online,    is_pwd_changed,
                cif_no,    cif_acct_name,    open_ibs,    open_mbs,    open_sms,
                group_id_mbs, GROUP_ID_SMS,    mobile_mbs,    mobile_sms,
                region_code,    brand_code,    login_flag,    token_no,    token_exp,
                transaction,    img_profile,    is_first_exp,    noti_pwd,    lasttime_pwd_changed,    security_type_mb,    open_sms_bdsd
            ) 
            select trunc(sysdate) POSTED_DT , user_id,    user_name,
                nick,    gender,    group_id,
                trade_pwd,    security_type,    sign_org,
                cert_type,    cert_name,    cert_code,
                cert_issued_date,    cert_issued_place,    telephone,
                mobile,    fax,    address,    postal_code,
                email,    open_acct_stmt,    acct_stmt_frq,    acct_stmt_method,
                open_mobile,    service_type,    receive_sms_adv,
                remark,    create_by,    create_by_mng,    create_time,    update_by,
                update_by_mng,    update_time,    status,    freezed_start_time,
                freezed_end_time,    login_count,    is_online,    is_pwd_changed,
                cif_no,    cif_acct_name,    open_ibs,    open_mbs,    open_sms,
                group_id_mbs, GROUP_ID_SMS,    mobile_mbs,    mobile_sms,
                region_code,    brand_code,    login_flag,    token_no,    token_exp,
                transaction,    img_profile,    is_first_exp,    noti_pwd,    lasttime_pwd_changed,    security_type_mb,    open_sms_bdsd
                from BC_USER_INFO ';
             EXECUTE IMMEDIATE v_InsertHisQueyr;
           -- xoa cac partition cu
             DECLARE
            cursor in_params IS ( 
                    select 'alter table BC_USER_INFO_HIS drop partition ' || part_name.partition_name || ' update global indexes' drop_query
                            from (select partition_name
                                from (
                                select KEEP_DATA_HIS.fn_partition_is_earlier(p.table_name, p.partition_position, to_date(v_dropParDate, 'YYYYMMDD')) should_drop_flag, p.*
                                from all_tab_partitions p
                                where table_name = 'BC_USER_INFO_HIS' and p.PARTITION_POSITION > 1
                                )
                                where should_drop_flag = 1) part_name
                ); 
             BEGIN
                  for pp in in_params loop
                      dbms_output.put_line('statement :' || pp.drop_query);
                      EXECUTE IMMEDIATE pp.drop_query;
                  end loop;
             END;
  END keep_his_bc_user_info;

  

END KEEP_DATA_HIS;

/
