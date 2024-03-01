--------------------------------------------------------
--  DDL for Package Body PK_PROCESS_FD
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_PROCESS_FD" AS

  PROCEDURE SYNC_DATA_FD (
    P_CIF_NO NUMBER
  )
  AS
  BEGIN
    FOR data_rec IN (
      select (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                   b.cdterm,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   (case when length(b.cdnum) = 13 then LPAD (b.cdnum, 14, '0') else TO_CHAR(b.cdnum) end) cdnum,
                                   DECODE(issdt,0,null,TO_DATE (b.issdt, 'yyyyddd')) issdt,
                                   DECODE(matdt,0,null,TO_DATE (b.matdt, 'yyyyddd')) matdt,
                                   fn_get_account_status_code(b.status) status,
                                   TRIM (b.TYPE) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   b.rate,
                                   b.brn from sync_etl_cdmast b 
      where b.cifno = P_CIF_NO and ACCTNO not in (
        select to_number(receipt_no) from BK_RECEIPT_INFO
      )
    )
    LOOP
      INSERT INTO BK_RECEIPT_INFO(RECEIPT_NO,PRODUCT_CODE,ACCOUNT_NO,PRINCIPAL,INTEREST_RATE,INTEREST_AMOUNT,TERM,OPENING_DATE,SETTLEMENT_DATE,IS_ROLLOUT_INTEREST,INTEREST_RECEIVE_ACCOUNT,STATUS,CDTCOD)
      VALUES (data_rec.acctno, data_rec.product_type, data_rec.cdnum, data_rec.cbal, data_rec.rate, data_rec.accint, 
      data_rec.cdterm, data_rec.issdt, data_rec.matdt, data_rec.renew, data_rec.dactn, data_rec.status, null);
    END LOOP;
    commit;
  END SYNC_DATA_FD;
  
  PROCEDURE SYNC_DATA_FD_BK_ACCOUNT (P_CIF_NO NUMBER)
  AS
  BEGIN
    FOR data_rec IN (
      SELECT
      (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
      (case when length(b.brn) <= 2 then LPAD (b.brn, 3, '0') else TO_CHAR(b.brn) end) brn,
      b.cifno,
      b.cdterm,
      TRIM (b.curtyp) curtyp,
      b.cbal,
      b.orgbal,
      b.accint,
      (case when length(b.cdnum) = 13 then LPAD (b.cdnum, 14, '0') else TO_CHAR(b.cdnum) end) cdnum,
      b.penamt,
      b.hold,
      DECODE(b.issdt,0,null,TO_DATE ( b.issdt, 'yyyyddd')) issdt,
      DECODE(b.matdt,0,null,TO_DATE (b.matdt, 'yyyyddd')) matdt,
      trim(b.acname) acname,
      fn_get_account_status_code(b.status) status,
      TRIM (b.TYPE) product_type,
      TRIM (b.renew) renew,
      b.dactn,
      TRIM(b.rate) rate,
      b.wdrwh
      FROM   sync_etl_cdmast b
      WHERE       TRIM (b.TYPE) IS NOT NULL
      AND b.acctno <> 0
      AND TRIM (b.status) IS NOT NULL
      AND  TRIM (b.cifno) in (P_CIF_NO)
      -- AND b.acctno in (11010100529901,11010100529871,11010100529983,11010100529741,11010100529723)
      AND b.acctno not in (select to_number(acct_no) from bk_account_info where acct_type='FD')
    )
    LOOP
      INSERT INTO bk_account_info (acct_no,
                                     bank_no,
                                     org_no,
                                     branch_no,
                                     cif_no,
                                     acct_type,
                                     currency_code,
                                     original_balance,
                                     principal_balance,
                                     accured_interest,
                                     p_acct_no,
                                     penalty_amount,
                                     hold_amount,
                                     issued_date,
                                     maturity_date,
                                     acct_name,
                                     product_type,
                                     interest_rate,
                                     status  )
                    VALUES   (data_rec.acctno,
                              '302',
                              data_rec.brn,
                              data_rec.brn,
                              data_rec.cifno,
                              'FD',
                              data_rec.curtyp,
                              data_rec.orgbal,
                              data_rec.cbal,
                              data_rec.accint,
                              data_rec.cdnum,
                              data_rec.penamt,
                              data_rec.hold,
                              data_rec.issdt,
                              data_rec.matdt,
                              data_rec.acname,
                              data_rec.product_type,
                              data_rec.rate,
                              data_rec.status );
    END LOOP;
    commit;
  END;

END PK_PROCESS_FD;

/
