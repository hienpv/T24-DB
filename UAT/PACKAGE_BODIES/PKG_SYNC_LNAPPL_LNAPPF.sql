--------------------------------------------------------
--  DDL for Package Body PKG_SYNC_LNAPPL_LNAPPF
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PKG_SYNC_LNAPPL_LNAPPF" AS

  PROCEDURE sync_lnappl AS
     v_result varchar2(20);
  BEGIN
	
	merge into cdc_lnappl t
        using ( select AABRN, AAAPNO, AACIFN, AAAPD6, AAAPD7, AASNME, AAFSNM, AACLMT, AACURR
                        from  stdattrn.lnappl@dblink_data1 
                    where aacifn in (501638,293188,2279706, 724709,952297)
        ) src
        on (t.AACIFN = src.AACIFN AND t.AAAPNO = src.AAAPNO)
    WHEN MATCHED THEN
        update set 
        AAAPD6 = src.AAAPD6,
          AAAPD7 = src.AAAPD7,
         AASNME = src.AASNME,
           AAFSNM = src.AAFSNM,
              AACLMT = src.AACLMT,
                AACURR = src.AACURR,
         ENTRY_DATE = sysdate
       WHEN NOT MATCHED THEN INSERT (
            t.AABRN, t.AAAPNO, t.AACIFN, t.AAAPD6, t.AAAPD7, t.AASNME, t.AAFSNM, t.AACLMT, t.AACURR, t.ENTRY_DATE) VALUES (
           src.AABRN, src.AAAPNO, src.AACIFN, src.AAAPD6, src.AAAPD7, src.AASNME, src.AAFSNM, src.AACLMT, src.AACURR, sysdate);    

    commit;
  END sync_lnappl;
  
  
  PROCEDURE sync_lnappf AS
     v_result varchar2(20);
  BEGIN
	
	merge into cdc_lnappf t
        using ( select AFAPNO, AFFCDE, AFSEQ, AFBKN, AFBRN, AFCUR, AFCPNO, AFCIFN, AFLEVL, AFISTS, AFEXP6, AFEXP7
                        from  stdattrn.lnappf@dblink_data1 
                    where AFCIFN in (501638,293188,2279706, 724709,952297)
        ) src
        on (t.AFCIFN = src.AFCIFN AND t.AFAPNO = src.AFAPNO)
    WHEN MATCHED THEN
        update set 
        AFFCDE = src.AFFCDE,
          AFSEQ = src.AFSEQ,
         AFBKN = src.AFBKN,
           AFBRN = src.AFBRN,
              AFCUR = src.AFCUR,
                AFCPNO = src.AFCPNO,
                AFLEVL = src.AFLEVL,
                AFISTS = src.AFISTS,
                AFEXP6 = src.AFEXP6,
                AFEXP7 = src.AFEXP7,
         ENTRY_DATE = sysdate
       WHEN NOT MATCHED THEN INSERT (
            t.AFAPNO, t.AFFCDE, t.AFSEQ, t.AFBKN, t.AFBRN, t.AFCUR, t.AFCPNO, t.AFCIFN, t.AFLEVL, t.AFISTS, t.AFEXP6, t.AFEXP7, t.ENTRY_DATE) VALUES (
           src.AFAPNO, src.AFFCDE, src.AFSEQ, src.AFBKN, src.AFBRN, src.AFCUR, src.AFCPNO, src.AFCIFN, src.AFLEVL, src.AFISTS, src.AFEXP6, src.AFEXP7, sysdate);    

    commit;
  END sync_lnappf;
  
  
  PROCEDURE sync_lnappl_lnappf AS
  BEGIN
	sync_lnappl;
    sync_lnappf;	
  END sync_lnappl_lnappf;


END PKG_SYNC_LNAPPL_LNAPPF;

/
