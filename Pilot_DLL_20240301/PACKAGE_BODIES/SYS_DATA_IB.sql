--------------------------------------------------------
--  DDL for Package Body SYS_DATA_IB
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."SYS_DATA_IB" is

  PROCEDURE SYSMAP is
  
  begin
    delete from google_map;
    insert into google_map
      (id,
       name,
       comments,
       latitude,
       longitude,
       style_url,
       is_deleted,
       type,
       status,
       is_branch,
       city_name)
      (select id,
              name,
              comments,
              latitude,
              longitude,
              style_url,
              is_deleted,
              type,
              status,
              is_branch,
              city_name
         from google_map@dblink_wpt1);
    commit;
  end;
  
    PROCEDURE SYSRATE is
  
  begin
    delete from bk_fx_rate where receipt_ccy !='VND';
    insert into bk_fx_rate (receipt_ccy,ccy_buy_rate,ccy_sell_rate,sell_ccy)
select (select code from Exchange_Rate@dblink_wpt1 where id=rate.exchange_rate_id) currence_code,rate.buying,rate.selling,'VND'   from Exchange_Rate_Detail@dblink_wpt1 rate 
inner join 
(select max(id) id_set,exchange_rate_id from  Exchange_Rate_Detail@dblink_wpt1 where exchange_rate_id is not null group by exchange_rate_id) setting on rate.id=setting.id_set ;

    commit;
  end;

  PROCEDURE SYSLIMITCARD is
  
  begin
    delete from add_pack;
    insert into add_pack
      (amnd_state,
       amnd_date,
       amnd_officer,
       amnd_prev,
       id,
       serv_pack__oid,
       f_i,
       ccat,
       contr_type,
       name,
       serv_pack_type,
       serv_pack__id,
       priority,
       can_be_switched,
       default_is_active,
       is_ready,
       apply_rules)
      (select amnd_state,
              amnd_date,
              amnd_officer,
              amnd_prev,
              id,
              serv_pack__oid,
              f_i,
              ccat,
              contr_type,
              name,
              serv_pack_type,
              serv_pack__id,
              priority,
              can_be_switched,
              default_is_active,
              is_ready,
              apply_rules
         from msb.add_pack@dblink_way4);
    delete from ebpsrv;
    insert into ebpsrv
      (essvky, espdes, atmdlm, ecomday, poslmt, tflmt)
      select essvky, espdes, atmdlm, ecomday, poslmt, tflmt
        from msb.ebpsrv@dblink_way4;
    commit;
  
  end;

end SYS_DATA_IB;

/
