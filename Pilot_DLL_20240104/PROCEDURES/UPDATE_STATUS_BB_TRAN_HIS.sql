--------------------------------------------------------
--  DDL for Procedure UPDATE_STATUS_BB_TRAN_HIS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "IBS"."UPDATE_STATUS_BB_TRAN_HIS" is
err varchar2(900);
begin


  Begin
  
    MERGE INTO bk_fx_rate c
    
    USING (select code,
                  buying,
                  selling,
                  buying aveBuy,
                  0 ccyavg,
                  'VND' jfxbkc
             from EXCHANGE_RATE_DETAIL@DBLINK_WPT ratedt,
                  EXCHANGE_RATE@DBLINK_WPT        rate
            where ratedt.id in
                  (select id
                     from (select max(id) id, exchange_rate_id
                             from EXCHANGE_RATE_DETAIL@DBLINK_WPT
                            where trunc(create_Date) = trunc(sysdate - 1)
                            group by exchange_rate_id))
              and rate.id = ratedt.exchange_rate_id) a
    ON (a.code = TRIM(c.receipt_ccy))
    WHEN MATCHED THEN
      UPDATE
         SET c.ccy_buy_rate  = a.buying,
             c.ccy_sell_rate = a.selling,
             c.ccy_mid_rate  = a.aveBuy,
             c.ccy_avg_rate  = a.ccyavg,
             c.sell_ccy      = a.jfxbkc
    WHEN NOT MATCHED THEN
      INSERT
        (c.receipt_ccy,
         c.ccy_buy_rate,
         c.ccy_sell_rate,
         c.ccy_mid_rate,
         c.ccy_avg_rate,
         c.sell_ccy)
      VALUES
        (a.code, a.buying, a.selling, a.aveBuy, a.ccyavg, a.jfxbkc);
  
    COMMIT;
  exception
    when others then
      err := 'Err';
  end;

end UPDATE_STATUS_bb_Tran_His;

/
