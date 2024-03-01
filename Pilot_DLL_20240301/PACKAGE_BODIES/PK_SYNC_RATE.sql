--------------------------------------------------------
--  DDL for Package Body PK_SYNC_RATE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_SYNC_RATE" AS

  PROCEDURE SYNC_DATA_FX_RATE AS
  BEGIN
    FOR data_rec IN (
      select a.CURRENCY, trim(b.jfxitc) JFXITC, b.JFXBRT, b.JFXSRT, b.JFXMRT  
      from bb_currency_config a
      left join RAWSTAGE.SI_PAR_SSFXRT@RAWSTAGE_PRO_CORE b on a.CURRENCY = trim(b.jfxitc)
      where a.CURRENCY is not null and trim(b.jfxitc) is not null
    )
    LOOP
      update bb_currency_config
      set BUY_RATE = data_rec.JFXBRT,
      SELL_RATE = data_rec.JFXSRT,
      MID_RATE = data_rec.JFXMRT,
      UPDATE_TIME = sysdate
      where CURRENCY = data_rec.CURRENCY;
    END LOOP;
    commit;
  END SYNC_DATA_FX_RATE;

END PK_SYNC_RATE;

/
