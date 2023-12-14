--------------------------------------------------------
--  DDL for Procedure MANUAL_DONG_BO_BK_CIF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "IBS"."MANUAL_DONG_BO_BK_CIF" 
(
  PARAM1 IN VARCHAR2 
) AS 
BEGIN


--b1: dong bo cfmast 
INSERT INTO sync_cfmast
              SELECT /*+ ALL_ROWS */
               a.cfcifn,
               a.cfsscd,
               a.cfssno,
               a.cfbnkn,
               a.cfbrnn,
               a.cfna1,
               a.cfbir6,
               a.cfbird,
               a.cfbirp,
               a.cfcitz,
               a.cfindi,
               /*ADD for update tax code*/
               trim(a.taxcod)
              FROM   svdatpv51.cfmast@DBLINK_DATA a where a.cfcifn = PARAM1;
              

--b2: dong bao addrr
INSERT INTO sync_cfaddr
              SELECT a.cfcifn,
                     a.cfadsq,
                     a.cfna2
              FROM   svdatpv51.cfaddr@dblink_data a
              WHERE  a.cfcifn = PARAM1;
--

---b3: Dong bo bkcif
insert into bk_cif (
              cif_no, cert_type, cert_code,
              bank_no, org_no, cif_acct_name,
              birth_place,  country,  individual,
              id, sync_hist
            )
          
            select 
              TRIM(x.cfcifn), TRIM(x.cfsscd), TRIM(x.cfssno),
              --trim(a.cfbnkn), 
'302', LPAD(TRIM(x.cfbrnn), 3, 0),
              TRIM(x.cfna1), TRIM(x.cfbirp),  TRIM(x.cfcitz),
              TRIM(x.cfindi), 0, 1 
from (SELECT /*+ INDEX(sync_cfmast, IDX_SYNC_CFMAST) */
                    a.cfcifn, a.cfsscd, a.cfssno, a.cfbnkn,
                    a.cfbrnn,  a.cfna1, a.cfbir6,
                    a.cfbird, a.cfbirp, a.cfcitz,
                    a.cfindi,
                    /*ADD for taxcode*/
                    a.taxcod,
                    b.cfna2
                   FROM   sync_cfmast a,
                          sync_cfaddr b
                   WHERE  length(rtrim(a.cfssno)) < 40 --fix length of char db2 always max
                   AND    b.cfadsq = (SELECT MAX(c.cfadsq)
                                      FROM   sync_cfaddr c
                                      WHERE  c.cfcifn = b.cfcifn)
                   AND    a.cfcifn = b.cfcifn) x;

-- xoa bang tam

delete sync_cfmast;
delete sync_cfaddr;

commit;

END MANUAL_DONG_BO_BK_CIF;

/
