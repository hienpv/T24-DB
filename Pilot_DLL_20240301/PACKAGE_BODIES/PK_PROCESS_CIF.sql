--------------------------------------------------------
--  DDL for Package Body PK_PROCESS_CIF
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_PROCESS_CIF" AS

  PROCEDURE SYNC_DATA_BK_CIF AS
  BEGIN
    FOR user_rec IN (
        select trim(d.CFCIFN) CIF_NO, trim(d.CFSSCD) CERT_TYPE, trim(d.CFSSNO) CERT_CODE, '302' BANK_NO, 
        CASE WHEN LENGTH(trim(d.cfbrnn)) = 2 then ('0' || trim(d.cfbrnn)) else trim(d.cfbrnn) end ORG_NO, 
        trim(d.cfna1) CIF_ACCT_NAME, case when d.cfbird =0 then null else TO_DATE(d.cfbird,'yyyyddd') end BIRTH_DATE,
        trim(d.CFBIRP) BIRTH_PLACE, trim(d.CFCITZ) COUNTRY, trim(d.CFINDI) INDIVIDUAL,
        c.TELEPHONE TELEPHONE, c.MOBILE MOBILE, c.ADDRESS ADDR, c.POSTAL_CODE POSTAL_CODE, c.EMAIL EMAIL, 0 as SYNC_HIST, 0 as ID, trim(d.taxcod) TAXCOD
        from (
          select a.* from (
            select cif_no, TELEPHONE, MOBILE, ADDRESS, POSTAL_CODE, EMAIL  
            from bb_corp_info
            where status = 'ACTV'
          ) a 
          where a.cif_no not in (select cif_no from bk_cif)
        ) c
        left join (select * from STAGING.SI_DAT_CFMAST@STAGING_PRO_CORE where trim(CFINDI)='N') d on to_number(c.cif_no) = trim(d.CFCIFN)
        where c.cif_no is not null and d.CFCIFN is not null and rownum <= 1000
    )
    LOOP
      insert into BK_CIF (CIF_NO, CERT_TYPE, CERT_CODE, BANK_NO, ORG_NO, CIF_ACCT_NAME, BIRTH_DATE, BIRTH_PLACE, COUNTRY, INDIVIDUAL,
        TELEPHONE, MOBILE, ADDR, POSTAL_CODE, EMAIL, SYNC_HIST, ID, TAXCOD)
        values (user_rec.CIF_NO, user_rec.CERT_TYPE, user_rec.CERT_CODE, user_rec.BANK_NO, user_rec.ORG_NO, user_rec.CIF_ACCT_NAME, user_rec.BIRTH_DATE, user_rec.BIRTH_PLACE, user_rec.COUNTRY, user_rec.INDIVIDUAL,
        user_rec.TELEPHONE, user_rec.MOBILE, user_rec.ADDR, user_rec.POSTAL_CODE, user_rec.EMAIL, user_rec.SYNC_HIST, user_rec.ID, user_rec.TAXCOD);
    END LOOP;
    commit;
  END SYNC_DATA_BK_CIF;


END PK_PROCESS_CIF;

/
