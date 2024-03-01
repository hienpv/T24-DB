--------------------------------------------------------
--  DDL for Procedure PROC_UPDATE_FD_PRODUCT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PROC_UPDATE_FD_PRODUCT" is 
  err varchar2(2000);
  cursor curMSG_OUT is
    select trim(ptype) product_code,
           pdesc,
           psdsc,
           pgroup,
           PYRCOD,
           PRATE,
           f.JRCRAT as rate,
           PMINAM,
           f.JRRATN,
           trim(f.JRRTBK) as term
      -- from SVPARPV51.cdpar2@DBLINK_DATA d
      -- inner join SVPARPV51.ssrate@DBLINK_DATA f
       from RAWSTAGE.SI_PAR_CDPAR2@RAWSTAGE_PRO_CORE d
       inner join RAWSTAGE.SI_PAR_SSRATE@RAWSTAGE_PRO_CORE f
        on d.PRATEN = f.JRREFR
           and trim(d.pcurty) = trim(f.JRRCUR)
           and CASE WHEN PRNRT9 =0 THEN 
                CASE WHEN PRNRT8 =0 THEN 
                    CASE WHEN PRNRT7 =0 THEN 
                        CASE WHEN PRNRT6 =0 THEN 
                             CASE WHEN PRNRT5 =0 THEN 
                                 CASE WHEN PRNRT4 =0 THEN 
                                     CASE WHEN PRNRT3 =0 THEN 
                                         CASE WHEN PRNRT2 =0 THEN 
                                             CASE WHEN PRNRT1 =0 THEN PRATEN
                                            ELSE PRNRT1 END 
                                         ELSE PRNRT2 END 
                                     ELSE PRNRT3 END 
                                ELSE PRNRT4 END 
                             ELSE PRNRT5 END 
                        ELSE PRNRT6 END 
                    ELSE PRNRT7 END
                ELSE PRNRT8 END 
            ELSE 
                PRNRT9 
            END = f.JRRATN
     where PCURTY = 'VND'
       and trim(ptype) in (select core_product_code from bk_receipt_product);
  v_MSG_OUT curMSG_OUT%rowtype;
BEGIN
 
  OPEN curMSG_OUT;
  LOOP
    -- Lay tong dong du lieu ca cursor de xu ly
    Begin
      FETCH curMSG_OUT INTO v_MSG_OUT;
      EXIT WHEN curMSG_OUT %notfound;
      -- update data bk_receipt_product
	  update bk_receipt_product pr
         set pr.rate = v_MSG_OUT.rate, pr.update_time = sysdate
       where pr.core_product_code = v_MSG_OUT.product_code 
       and CONCAT(pr.TERM, pr.TERM_CODE) = v_MSG_OUT.term;
      commit;
    Exception
      when others then
        err := sqlerrm;
    end;
 
  END LOOP;
 
  CLOSE curMSG_OUT;
end proc_update_fd_product;

/
