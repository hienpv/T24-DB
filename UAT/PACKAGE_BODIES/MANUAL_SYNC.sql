--------------------------------------------------------
--  DDL for Package Body MANUAL_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "MANUAL_SYNC" AS

  PROCEDURE merge_manual AS
  BEGIN
     MERGE INTO   bk_account_info c
                        USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
                                       a  .bankno,
                                          a.branch,
                                          LPAD (a.acctno, 14, '0') acctno,
                                          TRIM (a.actype) actype,
                                          TRIM (a.ddctyp) ddctyp,
                                          a.cifno,
                                          DECODE (a.status,
                                                  '1', 'ACTV',
                                                  --ACTIVE
                                                  '2', 'CLOS',
                                                  --CLOSED
                                                  '3', 'MATU',
                                                  --MATURED
                                                  '4', 'ACTV',
                                                  --New Record
                                                  '5', 'ACTZ',
                                                  --Active zero balance
                                                  '6', 'REST',
                                                  --RESTRICTED
                                                  '7', 'NOPO',
                                                  --NO POST
                                                  '8', 'COUN',
                                                  --Code unavailable
                                                  '9', 'DORM',
                                                  --DORMANT
                                                  '')
                                              status,
                                          a.datop7,
                                          a.hold,
                                          a.cbal,
                                          a.odlimt,
                                          a.whhirt,
                                          TRIM (a.sccode) sccode,
                                          TRIM (a.acname) acname,
                                          a.rate,
                                          a.accrue
                                   FROM   stdattrn.ddmast@dblink_data1 a
where acctno in ('4031010050275','3031011110197', '11031011502238', '3131010002711', '11031011502247')  ) src
                           ON   (src.acctno = c.acct_no)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.org_no = LPAD (src.branch, 3, '0'),
                           c.branch_no = LPAD (src.branch, 3, '0'),
                           c.currency_code = src.ddctyp,
                           c.bank_no = '302',       --a.bankno, core: 27, ibs:302
                           c.status = src.status,
                           c.hold_amount = src.hold,
                           c.ledger_balance = src.cbal,
                           c.available_balance = src.cbal - src.hold + src.odlimt,
                           c.overdraft_limit = src.odlimt,
                           --c.interest_rate     = src.whhirt,
                           c.acct_name = trim(src.acname),                          
                           c.acct_type = DECODE (src.actype, 'S', 'SA', 'CA'), --s:sa, d:ca
                           c.issued_date = TO_DATE (src.datop7, 'yyyyddd'),
                           c.cif_no = src.cifno,
                           c.interest_rate = src.rate,
                           c.accured_interest = src.accrue
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.acct_no,
                                            c.acct_name,
                                            c.p_acct_no,
                                            c.bank_no,
                                            c.org_no,                                           
                                            c.branch_no,
                                            c.sub_branch_no,
                                            c.acct_type,
                                            c.sub_acct_type,
                                            c.currency_code,
                                            c.available_balance,
                                            c.status,
                                            c.create_time,
                                            c.update_time,
                                            c.cif_no,
                                            c.ledger_balance,
                                            c.hold_amount,
                                            c.date_account_opened,
                                            c.overdraft_limit,
                                            c.issued_date,
                                            c.interest_rate,
                                            c.accured_interest)
                           VALUES   (LPAD (src.acctno, 14, '0'),
                                     trim(src.acname),
                                     NULL,
                                     '302',
                                     --a.bankno,
                                     LPAD (src.branch, 3, '0'),
                                     --a.branch,                                   
                                     LPAD (src.branch, 3, '0'),
                                     NULL,
                                     DECODE (src.actype, 'S', 'SA', 'CA'),
                                     NULL,
                                     src.ddctyp,
                                     (src.cbal - src.hold + src.odlimt),
                                     src.status,
                                     SYSDATE,
                                     SYSDATE,
                                     src.cifno,
                                     src.cbal,
                                     src.hold,
                                     NULL,
                                     src.odlimt,
                                     TO_DATE (src.datop7, 'yyyyddd'),
                                     src.rate,
                                     src.accrue);
                   COMMIT;
  END merge_manual;
  
  
  
 PROCEDURE merge_aaa_lnappl AS
  BEGIN
     MERGE INTO   cdc_lnappl c
                        USING   (select 
					trim(AABRN) AABRN,trim(AAAPNO) AAAPNO, 
					trim(AACIFN) AACIFN, trim(AAAPD6) AAAPD6, 
					trim(AAAPD7) AAAPD7, trim(AASNME) AASNME, 
					trim(AAFSNM) AAFSNM , trim(AABUMI) AABUMI, 
					trim(AAMARS) AAMARS, trim(AADECN) AADECN, 
					trim(AAM1H) AAM1H, trim(AAQ1) AAQ1 , 
					trim(AACCBR) AACCBR, trim(AAEXC) AAEXC, 
					trim(AAREV6) AAREV6, trim(AAREV7) AAREV7, 
					trim(AAREVN) AAREVN , trim(AARETN) AARETN, 
					trim(AAEXMP) AAEXMP, trim(AASIC1) AASIC1, 
					trim(AASIC2) AASIC2, trim(AASIC3) AASIC3, 
					trim(AASIC4) AASIC4, trim(AAWRKE) AAWRKE , 
					trim(AARFIN) AARFIN, trim(AACLMT) AACLMT, 
					trim(AACURR) AACURR, trim(AARRMK) AARRMK, trim(AACPNO) AACPNO , 
					trim(AACARC) AACARC, trim(AACSD6) AACSD6, trim(AACSD7) AACSD7, 
					trim(AASPFL) AASPFL , trim(AASUSP) AASUSP, trim(AAFBNK) AAFBNK, 
					trim(AAOFFL) AAOFFL, 
					trim(AACCID) AACCID, trim(AACCD7) AACCD7, 
					trim(AACCD6) AACCD6, trim(AACCTM) AACCTM, trim(AAMUID) AAMUID, trim(AAMWID) AAMWID, 
					trim(AAMDT6) AAMDT6, trim(AAMDT7) AAMDT7, 
					trim(AAMTIM) AAMTIM 
					FROM   stdattrn.lnappl@dblink_data1
where AACIFN in (2180409, 582775, 2180232, 2280075)  ) src
                           ON   (src.AACIFN = c.AACIFN and src.AAAPNO = c.AAAPNO)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.AACLMT = trim(src.AACLMT)
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.AABRN,   c.AAAPNO,
                                            c.AACIFN,    c.AAAPD6,
                                            c.AAAPD7, c.AASNME,
                                            c.AAFSNM, c.AACLMT,
											c.AACURR, change_time, entry_date)
                           VALUES   (src.AABRN,   src.AAAPNO,
                                            src.AACIFN,    src.AAAPD6,
                                            src.AAAPD7, src.AASNME,
                                            src.AAFSNM, src.AACLMT,
                                            src.AACURR, sysdate, sysdate);
                   COMMIT;
  END merge_aaa_lnappl;



END MANUAL_SYNC;

/
