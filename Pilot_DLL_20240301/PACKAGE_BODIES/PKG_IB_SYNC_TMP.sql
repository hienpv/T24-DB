--------------------------------------------------------
--  DDL for Package Body PKG_IB_SYNC_TMP
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."PKG_IB_SYNC_TMP" is

  Procedure Get_IB_RM IS
    vErr varchar2(900);
  Begin
    delete from bk_Ol2_sync_temp;
    Insert into bk_Ol2_sync_temp
      Select (a.rmdis7 * 1000000 + a.RMMTIM) RMMTIM,
             b.tlbf09,
             b.tlbf01,
             a.rmbena,
             a.rmamt rmamt,
             a.rmacno,
             b.tlbrmk,
             c.rdefth
        FROM SVDATPV51.RMMAST@DBLINK_DATA A,
             SVDATPV51.TLLOG@DBLINK_DATA  B,
             SVDATPV51.RMDETL@DBLINK_DATA C
       WHERE b.TLBTCD = 'IB8277'
         and RMRCID <> 'D'
         and b.TLTXOK = 'Y'
         and a.rmprdc = 'OL2'
         and c.RDRACT = '280898010'
         and b.tlbf01 = a.rmacno
         and c.rdacct = b.tlbf01;
    commit;
  
    insert into bb_transaction_info i
      (tran_sn, rm_no, create_time)
      select distinct h.tran_sn, t.rmacno, h.create_time
        from bb_transfer_history h
       INNER join bk_Ol2_sync_temp t
          on t.tlbf09 = ltrim(h.rollout_acct_no, '0')
         and t.rmamt =
             decode(h.paid_fee_source, 0, h.amount, h.amount - h.fee)
         and trim(t.rmbena) = trim(h.bnfc_acct_no)
         and trim(h.remark)=trim(substr(t.tlbrmk,26))
       where create_time > trunc(sysdate)
         and is_inter_bank = 'Y'
         and status = 'SUCC'
         
         and tran_sn not in (select tran_sn from bb_transaction_info);
    commit;
  
  Exception
    when others then
      vErr := sqlerrm;
  End Get_IB_RM;

end PKG_IB_SYNC_TMP;

/
