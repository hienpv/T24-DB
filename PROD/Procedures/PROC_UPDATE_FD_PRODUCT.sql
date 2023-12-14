--------------------------------------------------------
--  DDL for Procedure PROC_UPDATE_FD_PRODUCT
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "IBS"."PROC_UPDATE_FD_PRODUCT" is

  err varchar2(2000);
  cursor curMSG_OUT is
    select ptype,
           pdesc,
           psdsc,
           pgroup,
           PYRCOD,
           PRATE,
           f.JRCRAT Pratem,
           PMINAM
      from SVPARPV51.cdpar2@DBLINK_DATA d
     inner join SVPARPV51.ssrate@DBLINK_DATA f
        on CASE WHEN PRNRT9 =0 THEN 
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
       and d.pcurty = f.JRRCUR
     where PCURTY = 'VND'
       and trim(ptype) in (select product_code from bk_receipt_product);
  v_MSG_OUT curMSG_OUT%rowtype;
BEGIN

  OPEN curMSG_OUT;
  LOOP
    -- L?y t?ng dong d? li?u c?a cursor d? x? ly
    Begin
      FETCH curMSG_OUT
        INTO v_MSG_OUT;

      -- Thoat kh?i l?nh l?p n?u d? duy?t h?t t?t c? d? li?u
      EXIT WHEN curMSG_OUT %notfound;
      update bk_receipt_product pr
         set pr.rate = v_MSG_OUT.Pratem, pr.update_time = sysdate
       where trim(pr.product_code) = trim(v_MSG_OUT.Ptype);
      commit;
    Exception
      when others then
        err := sqlerrm;
    end;

  END LOOP;

  CLOSE curMSG_OUT;
end proc_update_fd_product;

/
