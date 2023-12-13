--------------------------------------------------------
--  DDL for Package Body PK_SYNC_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "PK_SYNC_DATA" AS
  PROCEDURE SYNC_DATA_SAVING_ONLINE(
    P_DATE DATE
  ) AS
  BEGIN
  
    delete sync_ddtnew; 
    commit;
    
    INSERT INTO   sync_ddtnew
            SELECT /*+ ALL_ROWS */
             a.bankno,
             a.branch,
             a.acctno,
             a.actype,
             a.ddctyp,
             a.cifno,
             a.status,
             a.datop7,
             a.hold,
             a.cbal,
             a.odlimt,
             a.whhirt,
             a.acname,
             '',
             a.sccode product_type,
             a.rate,
             a.accrue
              FROM stdattrn.ddtnew@dblink_data1 a
             WHERE 
                 a.cifno in (  select CIFNO from EB_SYN_TEMP   );
    COMMIT;
    
    MERGE INTO   bk_account_info c
    USING   (SELECT /*+ INDEX(sync_ddtnew, IDX_SYNC_DDTNEW) */
                   a  .bankno,
                      a.branch,
                      (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end) acctno,
                      a.actype,
                      a.ddctyp,
                      a.cifno,
                      DECODE (a.status,
                              '1', 'ACTV',
                              --ACTIVE
                              '2', 'CLOS',
                              --CLOSED
                              '3', 'MATU',
                              --MATURED
                              '4', 'ACTV',
                              --'NEWR', --New Record
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
                              'ACTV')
                          status,
                      a.datop7,
                      a.hold,
                      a.cbal,
                      a.odlimt,
                      a.whhirt,
                      trim(a.acname) acname,
                      TRIM (a.product_type) product_type,
                      a.rate,
                      a.accrue
               FROM   sync_ddtnew a
              WHERE                           --a.acctno <> 0
                   TRIM (a.status) IS NOT NULL
                   and SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '31'
                     
            ) src
       ON   (src.acctno = c.acct_no)
    WHEN MATCHED
    THEN
    UPDATE SET
       c.status = src.status,
       c.interest_rate = src.rate,
       c.accured_interest = src.accrue,
       c.product_type = src.product_type
    WHEN NOT MATCHED
    THEN
    INSERT              (c.bank_no,
                        c.org_no,
                        c.branch_no,
                        c.acct_no,
                        c.acct_type,
                        c.currency_code,
                        c.cif_no,
                        c.status,
                        c.establish_date,
                        c.hold_amount,
                        c.ledger_balance,
                        c.available_balance,
                        c.overdraft_limit,
                        c.interest_rate,
                        c.acct_name,
                        c.product_type,
                        c.issued_date,
                        c.accured_interest)
       VALUES   ('302',
                 --a.bankno,
                 (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                 (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                 (case when length(src.acctno) = 13 then LPAD (src.acctno, 14, '0') else TO_CHAR(src.acctno) end),
                 DECODE (TRIM (src.actype), 'S', 'SA', 'CA'),
                 TRIM (src.ddctyp),
                 src.cifno,
                 src.status,
                 TO_DATE (datop7, 'yyyyddd'),
                 src.hold,
                 src.cbal,
                 src.cbal - src.hold,
                 src.odlimt,
                 src.rate,
                 TRIM (src.acname),
                 src.product_type,
                 TO_DATE (datop7, 'yyyyddd'),
                 src.accrue);
    COMMIT;
    
    --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
--    MERGE INTO   bc_related_account c
--    USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
--                    LPAD (a.acctno, 14, '0') acctno,
--                        DECODE (a.status,
--                                '1', 'ACTV',
--                                --ACTIVE
--                                '2', 'CLOS',
--                                --CLOSED
--                                '3', 'MATU',
--                                --MATURED
--                                '4', 'ACTV',
--                                --New Record
--                                '5', 'ACTZ',
--                                --Active zero balance
--                                '6', 'REST',
--                                --RESTRICTED
--                                '7', 'NOPO',
--                                --NO POST
--                                '8', 'COUN',
--                                --Code unavailable
--                                '9', 'DORM',
--                                --DORMANT
--                                '')
--                            status,
--                        TRIM (a.actype) actype,
--                        b.user_id
--                FROM   sync_ddtnew a, bc_user_info b
--                WHERE       TO_CHAR (a.cifno) = b.cif_no
--                        AND b.status = 'ACTV'
--                        AND a.acctno <> 0
--                        AND TRIM (a.status) IS NOT NULL
--                        
--    
--            ) src
--        ON   (src.acctno = c.acct_no
--                AND src.user_id = c.user_id)
--    WHEN MATCHED
--    THEN
--    UPDATE SET c.status = src.status
--    WHEN NOT MATCHED
--    THEN
--    INSERT              (c.relation_id,
--                        c.user_id,
--                        c.acct_no,
--                        c.acct_type,
--                        --c.sub_acct_type,
--                        --c.alias,
--                        c.is_master,
--                        c.status,
--                        c.create_time)
--        VALUES   (seq_relation_id.NEXTVAL,
--                    src.user_id,
--                    src.acctno,
--                    DECODE (src.actype, 'S', 'SA', 'CA'),
--                    'N',
--                    src.status,
--                    SYSDATE);
--    
--    COMMIT;
    
    --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            
    MERGE INTO   bk_account_info c
        USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
                        a  .bankno,
                            a.branch,
                            (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end) acctno,
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
                            TRIM (a.product_type) product_type,
                            a.rate,
                            a.accrue
                    FROM   sync_ddmast a
                    WHERE   a.acctno <> 0
                            AND TRIM (a.status) IS NOT NULL
                        --  AND a.cifno >= g_min_count
                            and SUBSTR(LPAD(a.acctno, 14, '0'), 4, 2) <> '31'
                        --  AND a.cifno < g_max_count --AND    a.cifno = cif_tab(i)
        
                ) src
            ON   (src.acctno = c.acct_no)
    WHEN MATCHED
    THEN
        UPDATE SET
            c.org_no = (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
            c.branch_no = (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
            c.currency_code = src.ddctyp,
            c.bank_no = '302',       --a.bankno, core: 27, ibs:302
            c.status = src.status,
            c.hold_amount = src.hold,
            c.ledger_balance = src.cbal,
            c.available_balance = src.cbal - src.hold + src.odlimt,
            c.overdraft_limit = src.odlimt,
            --c.interest_rate     = src.whhirt,
            c.acct_name = trim(src.acname),
            c.product_type = src.product_type,
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
                            c.product_type,
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
            VALUES   ((case when length(src.acctno) = 13 then LPAD (src.acctno, 14, '0') else TO_CHAR(src.acctno) end),
                        trim(src.acname),
                        NULL,
                        '302',
                        --a.bankno,
                        (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
                        --a.branch,
                        src.product_type,
                        (case when length(src.branch) <= 2 then LPAD (src.branch, 3, '0') else TO_CHAR(src.branch) end),
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
    
    -- DDMEMO
     delete sync_ddmemo; 
     INSERT INTO sync_ddmemo
              ( --bankno,
               --branch,
               acctno,
               --actype,
               cifno,
               status,
               hold,
               cbal,
               odlimt,
               --sccode,
               acname)
              (SELECT /*+ ALL_ROWS */
               --NULL,
               --a.bankno,
               --NULL,
               --a.branch,
                a.acctno,
                --NULL,
                --TRIM(a.actype),
                a.cifno,
                a.status,
                a.hold,
                a.cbal,
                a.odlimt,
                --NULL,
                --TRIM(a.sccode),
                TRIM(a.acname)
                 FROM stdattrn.ddmemo@dblink_data1 a
               /*WHERE  LPAD(a.acctno,
               14,
               '0') IN (SELECT acct_no
                        FROM   sync_account_info)*/
                WHERE a.cifno   in  ( select CIFNO FROM EB_SYN_TEMP   ));  

            COMMIT;
      
      --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_account_info c
                 USING   (SELECT /*+ INDEX(sync_ddmemo, IDX_SYNC_DDMEMO) */
                                   --a.bankno,
                                   --a.branch,
                                   (case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end) acctno,
                                   --a.actype,
                                   --a.cifno,
                                   DECODE (a.status,
                                           '1', 'ACTV',
                                           --ACTIVE
                                           '2', 'CLOS',
                                           --CLOSED
                                           '3', 'MATU',
                                           --MATURED
                                           '4', 'ACTV',
                                           --'NEWR', --New Record
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
                                   a.hold,
                                   a.cbal,
                                   a.odlimt,
                                   a.sccode,
                                   a.acname
                            FROM   sync_ddmemo a
                           WHERE   a.acctno <> 0
                                   AND TRIM (a.status) IS NOT NULL
                                      
                         ) src
                    ON   (src.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET --c.org_no = LPAD(a.branch, 3, '0'),--a.branch,
                    --c.branch_no = LPAD(a.branch, 3, '0'),--a.branch,
                    --         c.currency_code     = trim(a.DDCTYP),
                    --         c.bank_no           = a.BANKNO,
                    c.status = src.status,
                    c.hold_amount = src.hold,
                    c.ledger_balance = src.cbal,
                    c.available_balance = src.cbal - src.hold + src.odlimt,
                    c.overdraft_limit = src.odlimt,
                    --        c.interest_rate     = a.WHHIRT,
                    c.acct_name = trim(src.acname);

            --        c.product_type      = a.product_type,
            --        c.passbook_no       = a.passbook_no,
            --        c.acct_type         = decode(a.ACTYPE,'S','SA','CA')
            COMMIT;
            
             -- CDGROU{P
     --
     delete sync_cfagrp;
     commit;
     INSERT INTO sync_cfagrp a
              (a.cfcifn, --cif
               a.cfagno, --acctno
               --a.CFAPCD,--  Application         code
               --a.CFGPBT,--  Passbook code
               a.cfgsts, --  CF Group status
               a.cfgnam, --  Group account name
               a.cfgcur, --  Group currency
               --a.CFGRAT,--  Group rate
               a.cfggbl, --  Group balance
               --a.CFGPBL,--  Passbook balance
               --a.CFGINT,--  Accrued interest
               --a.CFGPEN,--  Penalty amount
               --a.CFGHLD,--  Hold amount
               --a.CFGFLT,--  Float amount
               --a.CFGCFT,--  Collection float
               a.cfagty, --  Account             Group Type
               --a.CFAGTC,--  Account             product type
               a.cfagd7 --  Group Date          YYYYDDD
               --a.CFCMD7--  Maturity date       for CDs
               --a.CFABRN--  Branch
               )
              (SELECT b.cfcifn,
                      b.cfagno,
                      b.cfgsts,
                      b.cfgnam,
                      b.cfgcur,
                      b.cfggbl,
                      b.cfagty,
                      b.cfagd7
                 FROM stdattrn.cfagrp@dblink_data1 b
                WHERE /*b.cfgcur = 'VND' --AND    b.cfapcd = 'CD' -- for FD
                                                                                                                                --AND    TRIM(b.cfgsts) = 'N'
                                                                                                               AND*/
                TRIM(b.cfgcur) IS NOT NULL
               --AND    b.cfgcur = 'VND'
               /*AND    TRIM(b.cfcifn) IN (SELECT cif_no
               FROM   sync_cif_n)*/
            AND b.cfcifn   in (123456 ) );

            COMMIT;
  INSERT INTO sync_cif_n
      SELECT a.cif_no, 'Y'
        FROM (
              SELECT cif_no FROM bb_corp_info) a;

   

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_account_info c
                 USING   (SELECT   a.cfcifn,
                                  (case when length(a.cfagno) = 13 then LPAD (a.cfagno, 14, '0') else TO_CHAR(a.cfagno) end) cfagno,
                                   a.cfgsts,
                                   trim(a.cfgnam) cfgnam,
                                   a.cfgcur,
                                   a.cfggbl,
                                   a.cfagty,
                                   a.cfagd7
                            FROM   sync_cfagrp a
                           
                      )
                         src
                    ON   (src.cfagno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.acct_name = trim(src.cfgnam),
                    c.currency_code = TRIM (src.cfgcur),
                    c.issued_date =
                        DECODE (LENGTH (src.cfagd7),
                                7, TO_DATE (src.cfagd7, 'yyyyddd'),
                                NULL),
                    c.status =
                        DECODE (TRIM (src.cfgsts),
                                'N', 'ACTV',
                                'C', 'ACTV',
                                'CLOS'),
                    c.bank_no = '302',                          --tam thoi
                    c.branch_no = SUBSTR ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end), 0, 3)
            WHEN NOT MATCHED
            THEN
                INSERT              (c.acct_no,
                                     c.acct_name,
                                     c.cif_no,
                                     c.acct_type,
                                     c.currency_code,
                                     c.org_no,
                                     c.branch_no,
                                     c.establish_date,
                                     c.issued_date,
                                     c.status,
                                     c.update_time,
                                     c.bank_no)
                    VALUES   ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end),
                              trim(src.cfgnam),
                              src.cfcifn,
                              'FD',
                              TRIM (src.cfgcur),
                              SUBSTR ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end), 0, 3),
                              --tam thoi
                              SUBSTR ((case when length(src.cfagno) = 13 then LPAD (src.cfagno, 14, '0') else TO_CHAR(src.cfagno) end), 0, 3),
                              DECODE (LENGTH (src.cfagd7),
                                      7, TO_DATE (src.cfagd7, 'yyyyddd'),
                                      NULL),
                              DECODE (LENGTH (src.cfagd7),
                                      7, TO_DATE (src.cfagd7, 'yyyyddd'),
                                      NULL),
                              DECODE (TRIM (src.cfgsts),
                                      'N', 'ACTV',
                                      'C', 'ACTV',
                                      'CLOS'),
                              --'ACTV',
                              SYSDATE,
                              '302');

            COMMIT;

     -- CDTNEW
            delete sync_cdtnew ;
            commit;
            INSERT INTO   sync_cdtnew
              SELECT /*+ ALL_ROWS */
               b.bankno,
               b.brn,
               b.curtyp,
               b.cifno,
               b.orgbal,
               b.cbal,
               b.accint,
               b.penamt,
               b.hold,
               b.wdrwh,
               b.cdnum,
               b.issdt,
               b.matdt,
               b.rnwctr,
               b.status,
               b.acname,
               b.acctno,
               b.TYPE   product_type,
               b.rate,
               b.renew,
               b.dactn,
               b.cdterm,
               b.CDMUID
                FROM stdattrn.cdtnew@dblink_data1 b
               WHERE b.status <> 2 --AND b.curtyp = 'VND'
                 AND b.status <> 0
                    --AND    TRIM(b.type) IS NOT NULL;
                 
                 AND b.cifno in (  select CIFNO from EB_SYN_TEMP );

            COMMIT;
           
    delete sync_cif_n; 
    COMMIT;
    INSERT INTO    sync_cif_n 
 
       select CIFNO  , 'Y'from EB_SYN_TEMP  ;         
   COMMIT;
                   MERGE INTO   bk_receipt_info c
                        USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                                    (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                          b.cdterm,
                                          b.orgbal,
                                          b.accint,
                                          b.cdnum,
                                          issdt,
                                          matdt,
                                          DECODE (b.status,
                                                  '1', 'ACTV',
                                                  --ACTIVE
                                                  '2', 'CLOS',
                                                  --CLOSED
                                                  '3', 'MATU',
                                                  --MATURED
                                                  '4', 'ACTV',
                                                  --'NEWR', --New Record
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
                                          TRIM (b.product_type) product_type,
                                          TRIM (b.renew) renew,
                                          b.dactn,
                                          b.rate,
                                          b.cbal
                                   FROM   sync_cdtnew b
                                  WHERE    TRIM (b.status) IS NOT NULL
                                          AND TRIM (b.cifno) IN
                                                     (SELECT   cif_no
                                                        FROM   sync_cif_n
                                                      )
                                          AND TRIM (b.product_type) IS NOT NULL
                                           --AND    b.cifno = cif_tab(i)
                                                                          --and    b.rowid between min_rid_tab(i) and max_rid_tab(i)
                                ) a
                           ON   (a.acctno = c.receipt_no)
                   
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.receipt_no,
                                            c.product_code,
                                            c.account_no,
                                            c.principal,
                                            c.interest_rate,
                                            c.interest_amount,
                                            c.term,
                                            c.opening_date,
                                            c.settlement_date,
                                            c.is_rollout_interest,
                                            c.interest_receive_account,
                                            c.status)
                           VALUES   ((case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                                     a.product_type,
                                     (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),
                                     a.cbal,
                                     a.rate,
                                     a.accint,
                                     a.cdterm,
                                     TO_DATE (a.issdt, 'yyyyddd'),
                                     TO_DATE (a.matdt, 'yyyyddd'),
                                     TRIM (a.renew),
                                     a.dactn,
                                     a.status);

                   COMMIT;

                         MERGE INTO   bk_account_info c
                        USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                                      (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                          b.brn,
                                          b.cifno,
                                          b.cdterm,
                                          b.curtyp,
                                          b.cbal,
                                          b.orgbal,
                                          b.accint,
                                          b.cdnum,
                                          b.penamt,
                                          b.hold,
                                          b.issdt,
                                          b.matdt,
                                          trim(b.acname) acname,
                                          DECODE (b.status,
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
                                          TRIM (b.product_type) product_type,
                                          TRIM (b.renew) renew,
                                          b.dactn,
                                          b.rate,
                                          b.wdrwh
                                   FROM   sync_cdtnew b
                                  WHERE         TRIM (b.product_type) IS NOT NULL
                                          AND acctno <> 0
                                          AND TRIM (b.status) IS NOT NULL
                                          AND TRIM (b.cifno) IN
                                                     (SELECT   cif_no
                                                        FROM   sync_cif_n
                                                      )
                                                ) a
                           ON   (a.acctno = c.acct_no)
                   WHEN MATCHED
                   THEN
                       UPDATE SET
                           c.bank_no = '302',
                           c.org_no = (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                           c.cif_no = a.cifno,
                           c.acct_type = 'FD',
                           --c.term              = TRIM(a.cdterm),
                           c.currency_code = TRIM (a.curtyp),
                           c.original_balance = a.orgbal,
                           c.principal_balance = a.cbal,
                           c.accured_interest = a.accint,
                           c.p_acct_no = (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),
                           c.penalty_amount = a.penamt,
                           c.current_cash_value =
                               (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
                           c.hold_amount = a.hold,
                           c.issued_date = TO_DATE (a.issdt, 'yyyyddd'),
                           c.maturity_date = TO_DATE (a.matdt, 'yyyyddd'),
                           c.acct_name = trim(a.acname),
                           c.product_type = TRIM (a.product_type),
                           c.interest_rate = TRIM (a.rate),
                           c.status = a.status
                   WHEN NOT MATCHED
                   THEN
                       INSERT              (c.acct_no,
                                            c.bank_no,
                                            c.org_no,
                                            c.branch_no,
                                            c.cif_no,
                                            c.acct_type,
                                            --c.term,
                                            c.currency_code,
                                            c.original_balance,
                                           c.principal_balance,
                                            c.accured_interest,
                                            c.p_acct_no,
                                            c.penalty_amount,
                                            c.hold_amount,
                                            c.issued_date,
                                            c.maturity_date,
                                            c.acct_name,
                                            c.product_type,
                                            c.interest_rate,
                                            c.status,
                                            c.current_cash_value)
                           VALUES   ((case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                                     '302',
                                     (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                                     (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                                     a.cifno,
                                     'FD',
                                     --TRIM(a.cdterm),
                                     TRIM (a.curtyp),
                                     a.orgbal,
                                     a.cbal,
                                     a.accint,
                                     a.cdnum,
                                     a.penamt,
                                     a.hold,
                                     TO_DATE (a.issdt, 'yyyyddd'),
                                     TO_DATE (a.matdt, 'yyyyddd'),
                                     trim(a.acname),
                                     a.product_type,
                                     a.rate,
                                     a.status,
                                     (  a.cbal
                                      + a.accint
                                      - a.penamt
                                      - a.hold
                                      - a.wdrwh));

                   COMMIT;

                   --SAVEPOINT s_merge_bulk;

                   --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
--                   MERGE INTO   bc_related_account c
--                        USING   (SELECT /*+ INDEX(sync_ddmast, IDX_SYNC_DDMAST) */
--                                       LPAD (a.acctno, 14, '0') acctno,
--                                          DECODE (a.status,
--                                                  '1', 'ACTV',
--                                                  --ACTIVE
--                                                  '2', 'CLOS',
--                                                  --CLOSED
--                                                  '3', 'MATU',
--                                                  --MATURED
--                                                  '4', 'ACTV',
--                                                  --New Record
--                                                  '5', 'ACTZ',
--                                                  --Active zero balance
--                                                  '6', 'REST',
--                                                  --RESTRICTED
--                                                  '7', 'NOPO',
--                                                  --NO POST
--                                                  '8', 'COUN',
--                                                  --Code unavailable
--                                                  '9', 'DORM',
--                                                 --DORMANT
--                                                  '')
--                                              status,
--                                          b.user_id
--                                   FROM   sync_cdtnew a, bc_user_info b
--                                  WHERE       TO_CHAR (a.cifno) = b.cif_no
--                                          AND b.status = 'ACTV'
--                                          AND a.acctno <> 0
--                                          AND TRIM (a.status) IS NOT NULL
--                                          
--                                          AND TRIM (a.cifno) IN
--                                                     (SELECT   cif_no
--                                                        FROM   sync_cif_n
--                                                      )
--                                                      
--    
--                                ) src
--                           ON   (src.acctno = c.acct_no
--                                 AND src.user_id = c.user_id)
--                   WHEN MATCHED
--                   THEN
--                       UPDATE SET c.status = src.status
--                   WHEN NOT MATCHED
--                   THEN
--                       INSERT              (c.relation_id,
--                                            c.user_id,
--                                            c.acct_no,
--                                            c.acct_type,
--                                            --c.sub_acct_type,
--                                            --c.alias,
--                                            c.is_master,
--                                            c.status,
--                                            c.create_time)
--                           VALUES   (seq_relation_id.NEXTVAL,
--                                     src.user_id,
--                                     src.acctno,
--                                     'FD',
--                                     'N',
--                                     src.status,
--                                     SYSDATE);
--
--                   COMMIT;
        --CDMAST
    delete sync_cdmast ; 
     INSERT INTO sync_cdmast
              (bankno,
               brn,
               acctno,
               cifno,
               cdterm,
               CDTCOD,
               curtyp,
               orgbal,
               cbal,
               accint,
               penamt,
               hold,
               wdrwh,
               cdnum,
               issdt,
               matdt,
               rnwctr,
               status,
               acname,
               TYPE,
               rate,
               renew,
               dactn)
              (SELECT /* +ALL_ROWS */
                b.bankno,
                b.brn,
                b.acctno,
                b.cifno,
                b.cdterm,
                b.CDTCOD,
                b.curtyp,
                b.orgbal,
                b.cbal,
                b.accint,
                b.penamt,
                b.hold,
                b.wdrwh,
                b.cdnum,
                b.issdt,
                b.matdt,
                b.rnwctr,
                b.status,
                b.acname,
                b.TYPE,
                b.rate,
                b.renew,
                b.dactn
                 FROM stdattrn.cdmast@dblink_data1 b
                WHERE --((b.matdt > v_checkpoint_date AND b.status = 2) OR
                      -- ((b.stmdt > v_checkpoint_date AND b.status = 2) OR
                      b.status <> 2
                     -- )
                    
                  AND b.cifno in (select cifno from eb_syn_temp  )  );

            COMMIT;
            
          --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO   bk_receipt_info c
                 USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                 (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                   b.cdterm,
                                   b.CDTCOD,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   b.cdnum,
                                   issdt,
                                   matdt,
                                   DECODE (b.status,
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
                                   TRIM (b.TYPE) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   b.rate,
                                   b.brn
                            FROM   sync_cdmast b
                          
                           WHERE       TRIM (b.TYPE) IS NOT NULL
                                   AND acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                                   
                         ) a
                    ON   (a.acctno = c.receipt_no)
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.product_code = a.product_type,
                    c.principal = a.cbal,                      --a.orgbal,
                    c.interest_rate = a.rate,
                    c.interest_amount = a.accint,
                    c.term = a.cdterm,
                    c.CDTCOD=a.CDTCOD,
                    c.opening_date = TO_DATE (a.issdt, 'yyyyddd'),
                    c.settlement_date = TO_DATE (a.matdt, 'yyyyddd'),
                    c.is_rollout_interest = TRIM (a.renew),
                    c.interest_receive_account = a.dactn,
                    c.status = a.status,
                    c.account_no = (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end)
            WHEN NOT MATCHED
            THEN
                INSERT              (c.receipt_no,
                                     c.product_code,
                                     c.account_no,
                                     c.principal,
                                     c.interest_rate,
                                     c.interest_amount,
                                     c.term,
                                     c.Cdtcod,
                                     c.opening_date,
                                     c.settlement_date,
                                     c.is_rollout_interest,
                                     c.interest_receive_account,
                                     c.status)
                    VALUES   ((case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                              a.product_type,
                              (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),
                              --a.cdnum,
                              a.cbal,
                              a.rate,
                              a.accint,
                              a.cdterm,
                              a.CDTCOD,
                              TO_DATE (a.issdt, 'yyyyddd'),
                              TO_DATE (a.matdt, 'yyyyddd'),
                              TRIM (a.renew),
                              a.dactn,
                              a.status);

            COMMIT;
          MERGE INTO   bk_account_info c
                 USING   (SELECT /*+ INDEX(sync_cdmast_n, IDX_sync_cdmast) */
                              (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
                                   b.brn,
                                   b.cifno,
                                   b.cdterm,
                                   b.curtyp,
                                   b.cbal,
                                   b.orgbal,
                                   b.accint,
                                   b.cdnum,
                                   b.penamt,
                                   b.hold,
                                   b.issdt,
                                   b.matdt,
                                   trim(b.acname) acname,
                                   DECODE (b.status,
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
                                   TRIM (b.TYPE) product_type,
                                   TRIM (b.renew) renew,
                                   b.dactn,
                                   b.rate,
                                   b.wdrwh
                            FROM   sync_cdmast b
                           
                           WHERE       TRIM (b.TYPE) IS NOT NULL
                                   AND acctno <> 0
                                   AND TRIM (b.status) IS NOT NULL
                            ) a
                    ON   (a.acctno = c.acct_no)
            WHEN MATCHED
            THEN
                UPDATE SET c.bank_no = '302',
                           c.org_no = (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                           c.cif_no = a.cifno,
                           c.acct_type = 'FD',
                           --c.term              = TRIM(a.cdterm),
                           c.currency_code = TRIM (a.curtyp),
                           c.original_balance = a.orgbal,
                           c.principal_balance = a.cbal,
                           c.accured_interest = a.accint,
                           /*c.current_cash_value =
                           (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),*/
                           c.p_acct_no = (case when length(a.cdnum) = 13 then LPAD (a.cdnum, 14, '0') else TO_CHAR(a.cdnum) end),
                           c.penalty_amount = a.penamt,
                           c.hold_amount = a.hold,
                           c.issued_date = TO_DATE (a.issdt, 'yyyyddd'),
                           c.maturity_date = TO_DATE (a.matdt, 'yyyyddd'),
                           c.acct_name = trim(a.acname),
                           c.product_type = TRIM (a.product_type),
                           c.interest_rate = TRIM (a.rate),
                           c.status = a.status
            WHEN NOT MATCHED
            THEN
                INSERT              (c.acct_no,
                                     c.bank_no,
                                     c.org_no,
                                     c.branch_no,
                                     c.cif_no,
                                     c.acct_type,
                                     --c.term,
                                     c.currency_code,
                                     c.original_balance,
                                     c.principal_balance,
                                     c.accured_interest,
                                     c.p_acct_no,
                                     c.penalty_amount,
                                     c.hold_amount,
                                     c.issued_date,
                                     c.maturity_date,
                                     c.acct_name,
                                     c.product_type,
                                     c.interest_rate,
                                     c.status /*,
                                                                                                               c.current_cash_value*/
                                             )
                    VALUES   ((case when length(a.acctno) = 13 then LPAD (a.acctno, 14, '0') else TO_CHAR(a.acctno) end),
                              '302',
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                              (case when length(a.brn) <= 2 then LPAD (a.brn, 3, '0') else TO_CHAR(a.brn) end),
                              a.cifno,
                              'FD',
                              --TRIM(a.cdterm),
                              TRIM (a.curtyp),
                              a.orgbal,
                              a.cbal,
                              a.accint,
                              a.cdnum,
                              a.penamt,
                              a.hold,
                              TO_DATE (a.issdt, 'yyyyddd'),
                              TO_DATE (a.matdt, 'yyyyddd'),
                              trim(a.acname),
                              a.product_type,
                              a.rate,
                              a.status /*,(a.cbal + a.accint - a.penamt - a.hold - a.wdrwh)*/
                                      );

            COMMIT;
      -- CDMEMO
      delete sync_cdmemo;
      commit;
      INSERT INTO sync_cdmemo
          (acctno,
           curtyp,
           cbal,
           accint,
           penamt,
           hold,
           wdrwh,
           cdnum,
           status)
          (SELECT /*+ ALL_ROWS */
            b.acctno,
            TRIM(b.curtyp),
            b.cbal,
            b.accint,
            b.penamt,
            b.hold,
            b.wdrwh,
            b.cdnum,
            b.status
             FROM stdattrn.cdmemo@dblink_data1 b /*WHERE  LPAD(b.acctno,
                                                                                                                    14,
                                                                                                                    '0') IN (SELECT acct_no
                                                                                                                             FROM   sync_account_info)*/
           );

        COMMIT;
            
    MERGE INTO bk_receipt_info c
    USING (SELECT /*+ INDEX(sync_cdmemo, IDX_SYNC_CDMEMO) */
    (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
            --b.curtyp,
            b.cbal,
            b.accint,
            --b.penamt,
            --b.hold,
            --b.wdrwh,
            --b.cdnum,
            DECODE(b.status,
                   '1',
                   'ACTV',
                   --ACTIVE
                   '2',
                   'CLOS',
                   --CLOSED
                   '3',
                   'MATU',
                   --MATURED
                   '4',
                   'ACTV',
                   --'NEWR', --New Record
                   '5',
                   'ACTZ',
                   --Active zero balance
                   '6',
                   'REST',
                   --RESTRICTED
                   '7',
                   'NOPO',
                   --NO POST
                   '8',
                   'COUN',
                   --Code unavailable
                   '9',
                   'DORM',
                   --DORMANT
                   '') status
             FROM sync_cdmemo b
            WHERE b.acctno <> 0
              AND TRIM(b.status) IS NOT NULL
           /*AND    LPAD(b.acctno,
           14,
           '0') IN (SELECT receipt_no
                     FROM   bk_receipt_info)*/
           --AND    b.ROWID between min_rid_tab(i) and max_rid_tab(i)
           ) a
    ON (a.acctno = c.receipt_no)
    WHEN MATCHED THEN
      UPDATE
         SET --            c.bank_no               =   a.BANKNO,
             --            c.org_no                =   a.BRN,
             --            c.term                  =   a.CDTERM, Chua hieu term
             -- c.currency_code = a.curtyp,
             --            c.original_balance      =   a.ORGBAL,
             --c.principal_balance  = a.cbal,
             --c.accured_interest   = a.accint,
             --c.penalty_amount     = a.penamt,
             --c.hold_amount        = a.hold,
             --c.current_cash_value =
             --(a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
             --            c.issued_date           =   to_date(a.ISSDT6,'ddMMyy'),
             --            c.maturity_date         =   to_date(a.MATDT6,'ddMMyy'),
             --            c.times_nenewed_count   =   a.RNWCTR,
             --c.status = a.status,
             --            c.product_type          =   a.product_type,
             --c.acct_type = 'FD',
             --            c.acct_name             =   trim(a.ACNAME),
             --c.p_acct_no = a.cdnum;
             --c.product_code = a.product_type,
               c.principal = a.cbal, --a.orgbal,
             --c.interest_rate   = a.rate,
             c.interest_amount = a.accint,
             --c.term            = a.cdterm,
             --c.opening_date    = TO_DATE(a.issdt,
             --                            'yyyyddd'),
             --c.settlement_date = TO_DATE(a.matdt,
             --                            'yyyyddd'),
             --c.is_rollout_interest = TRIM(a.renew),
             --c.interest_receive_account = a.dactn,
             c.status = a.status;

    /* c.account_no               = LPAD(a.cdnum,
    14,
    '0'); */ 
    
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_account_info c
    USING (SELECT /*+ INDEX(sync_cdmemo, IDX_SYNC_CDMEMO) */
    (case when length(b.acctno) = 13 then LPAD (b.acctno, 14, '0') else TO_CHAR(b.acctno) end) acctno,
            --b.curtyp,
            b.cbal,
            b.accint,
            b.penamt,
            b.hold,
            b.wdrwh,
            b.cdnum,
            DECODE(b.status,
                   '1',
                   'ACTV',
                   --ACTIVE
                   '2',
                   'CLOS',
                   --CLOSED
                   '3',
                   'MATU',
                   --MATURED
                   '4',
                   'ACTV',
                   --'NEWR', --New Record
                   '5',
                   'ACTZ',
                   --Active zero balance
                   '6',
                   'REST',
                   --RESTRICTED
                   '7',
                   'NOPO',
                   --NO POST
                   '8',
                   'COUN',
                   --Code unavailable
                   '9',
                   'DORM',
                   --DORMANT
                   '') status
             FROM sync_cdmemo b
            WHERE b.acctno <> 0
              AND TRIM(b.status) IS NOT NULL
           /*AND    LPAD(b.acctno,
           14,
           '0') IN (SELECT receipt_no
                     FROM   bk_receipt_info)*/ --AND    b.ROWID between min_rid_tab(i) and max_rid_tab(i)
           /*AND    LPAD(b.acctno,
           14,
           '0') IN (SELECT acct_no
                     FROM   sync_account_info)*/
           ) a
    ON (a.acctno = c.acct_no)
    WHEN MATCHED THEN
      UPDATE
         SET --            c.bank_no               =   a.BANKNO,
             --            c.org_no                =   a.BRN,
             --            c.term                  =   a.CDTERM, Chua hieu term
             --  c.currency_code = a.curtyp,
               --c.original_balance = a.orgbal,
             c.principal_balance  = a.cbal,
             c.accured_interest   = a.accint,
             c.penalty_amount     = a.penamt,
             c.hold_amount        = a.hold,
             c.current_cash_value =
             (a.cbal + a.accint - a.penamt - a.hold - a.wdrwh),
             --            c.issued_date           =   to_date(a.ISSDT6,'ddMMyy'),
             --            c.maturity_date         =   to_date(a.MATDT6,'ddMMyy'),
             --            c.times_nenewed_count   =   a.RNWCTR,
         --    c.status = a.status, //anhnt6cdmemo
             --            c.product_type          =   a.product_type,
             c.acct_type = 'FD',
             --            c.acct_name             =   trim(a.ACNAME),
             c.p_acct_no = a.cdnum;

    COMMIT;
    -- TMTRAN
   
   -- TMTRAN
   
   
   delete sync_cif_n;
   commit; 
  
    INSERT INTO sync_cif_n
      SELECT cifno, 'N'
        FROM eb_syn_temp    ;
   delete sync_account_info;
   commit; 
    INSERT INTO sync_account_info a
      (SELECT bai.cif_no, bai.acct_no
         FROM bk_account_info bai
        WHERE bai.cif_no IN (SELECT cif_no FROM sync_cif_n));
   INSERT INTO sync_tmtrans
              SELECT /*+ ALL_ROWS */
               a.tmtxcd,
               a.tmresv07,
               a.tmdorc,
               a.tmtxamt,
               a.tmglcur,
               a.tmorgamt,
               a.tmtellid,
               a.tmefth,
               a.tmacctno,
               SYSDATE, --tminsdate,
               a.tmtellid,
               a.tmtxseq,
               a.tmtxstat,
               a.tmhosttxcd,
               a.tmapptype,
               a.tmeqvtrn,
               a.tmibttrn,
               a.tmsumtrn,
               '1', --tmtype
               'SUCC', --tmsts,
               a.tmentdt7,
               --'TM' || seq_core_sn_tm.nextval
               a.tmeffdt7,
               NULL,
               a.tmsseq,
               a.tmtiment
                FROM stdattrn.tmtran@dblink_data1 a
               WHERE (a.tmsumtrn IS NULL OR a.tmsumtrn <> 'N')
                 AND (a.tmibttrn IS NULL OR a.tmibttrn <> 'Y')
                 AND (a.tmeqvtrn IS NULL OR a.tmeqvtrn <> 'I')
                 AND a.tmapptype NOT IN ('S', 'G')
                 AND a.tmhosttxcd NOT IN (77, 129, 178, 179, 185)
                    --AND    a.tmtxstat <> 'CE' --giao dich huy
                 AND a.tmapptype IS NOT NULL
                 AND a.tmdorc IN ('D', 'C')
                 AND LENGTH(RTRIM(a.tmresv07)) = 14
                 AND LENGTH(RTRIM(a.tmresv07)) IS NOT NULL
                 --AND a.tmentdt7 >= (v_checkpoint_date - 1)
                 AND (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end) IN
                     (SELECT acct_no FROM sync_account_info)
               --   AND a.tmtiment BETWEEN g_min_time AND g_max_time
                 ;
          
            COMMIT;
            
      delete sync_tranmap ;
      commit; 
      
--      INSERT INTO sync_tranmap
--      SELECT /*+ ALL_ROWS */
--       a.tran_sn,
--       a.teller_id,
--       a.host_tran_sn,
--       a.host_real_date,
--       a.sender_id
--        FROM bec.bec_msglog@dblink_tranmap a
--      --FROM   bec.bec_msglog2 a
--       WHERE a.sorn = 'Y'  
--         AND TRUNC(TO_DATE(a.message_date, 'yyyymmddhh24mi')) >=
--             TRUNC(SYSDATE - 1)
--         AND LENGTH(a.tran_sn) < 21;
  
    --AND    TRUNC(TO_DATE(a.host_real_date,'yyyyddd')) = TRUNC(SYSDATE);
    MERGE INTO   bk_account_history c
           USING   (SELECT /*+ INDEX(sync_tmtrans, IDX_SYNC_TMTRAN) */
                                TO_DATE (
                                       (   a.tmentdt7
                                        || ':'
                                        || LPAD (a.tmtiment, 6, '0')),
                                       'yyyyddd:hh24miss')
                                       AS post_date,
                                   TO_DATE (
                                       (   a.tmeffdt7
                                        || ':'
                                        || LPAD (a.tmtiment, 6, '0')),
                                       'yyyyddd:hh24miss')
                                       AS tran_date,
                                   /*DECODE(length(TRIM(TMEFFDT7)), 7, TO_DATE(TMEFFDT7, 'yyyyddd'),
                                          NULL) AS tran_date,
                                   DECODE(length(TRIM(tmentdt7)), 7, TO_DATE(tmentdt7, 'yyyyddd'),
                                          NULL) AS post_date,*/
                                   TRIM (a.tmdorc) AS tmdorc,
                                   a.tmtxamt,
                                   TRIM (a.tmglcur) AS tmglcur,
                                   a.tmorgamt,
                                   DECODE (b.sender_id,
                                           NULL, 'CNT',
                                           b.sender_id)
                                       AS channel_id,
                                   --TRIM(a.tmefth) AS tmefth,
                                   SUBSTR (a.tmefth,
                                           11,
                                           LENGTH (a.tmefth))
                                       AS tmefth,
                                   a.tmacctno,
                                   SYSDATE AS tminsdate,
                                   TRIM (a.tmtellid) AS tmtellid,
                                   a.tmtxseq,
                                   /*'1',
                                   'SUCC',
                                   'SYNC', */
                                   b.tran_sn,
                                   a.tmtxstat,
                                   a.tmentdt7,
                                   a.tmresv07,
                                   d.tran_service_code,
                                   e.staff_name,
                                   a.tmhosttxcd,
                                   a.tmsseq,
                                   b.sender_id,
                                   TRIM (a.tmapptype) AS tmapptype
                            FROM   sync_tmtrans a,
                                   sync_tranmap b,
                                   sync_tran_code d,
                                   sync_teller e
                           WHERE   TRIM (a.tmtellid) = b.teller_id(+)
                                   AND TRIM (a.tmtxseq) =
                                          b.host_tran_sn(+)
                                   AND TRUNC(TO_DATE (a.tmentdt7,
                                                      'yyyyddd')) =
                                          TRUNC(TO_DATE (
                                                    b.host_real_date(+),
                                                    'yyyyddd'))
                                   AND (case when length(a.tmacctno) = 13 then LPAD (a.tmacctno, 14, '0') else TO_CHAR(a.tmacctno) end) IN
                                              (SELECT   x.acct_no
                                                 FROM   sync_account_info x)
                                   AND TRIM (a.tmtxcd) = d.tran_code(+)
                                   AND TRIM (a.tmtellid) =   e.staff_name(+)
                                  -- AND a.tmtiment BETWEEN g_min_time   AND  g_max_time
                                   )
                         src
                    ON   (    c.teller_id = src.tmtellid
                          AND c.tm_seq = src.tmtxseq
                          AND TRUNC (c.post_time) = TRUNC (src.post_date)
                          AND c.rollout_acct_no = (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end)
                          AND c.dc_sign = TRIM (src.tmdorc)
                          AND c.tran_device = TRIM (src.tmsseq)
                          AND c.device_no = TRIM (src.tmhosttxcd))
            WHEN MATCHED
            THEN
                UPDATE SET
                    c.status = 'SUCC' /* DECODE(src.tmtxstat,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              '',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'FAIL',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'PP',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'PEND',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'PT',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'SUCC',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'SUCC') */
                                     ,
                    c.tran_service_code =
                        DECODE (src.tran_service_code,
                                NULL, 'RTR001',
                                'RTR002'),
                    --c.tran_sn           = src.tran_sn,
                    /*c.channel = DECODE(src.staff_name,
                    NULL,
                    'CNT',
                    'IBS')*/
                    c.channel =
                        DECODE (src.sender_id,
                                NULL, 'CNT',
                                src.sender_id)
            WHEN NOT MATCHED
            THEN
                INSERT              (c.core_sn,
                                     c.tran_time,
                                     c.post_time,
                                     c.dc_sign,
                                     c.amount,
                                     c.currency_code,
                                     c.pre_balance,
                                     c.channel,
                                     c.remark,
                                     c.rollout_acct_no,
                                     c.insert_date,
                                     c.teller_id,
                                     c.tm_seq,
                                     c.sync_type,
                                     c.status,
                                     c.tc_code,
                                     c.tran_sn,
                                     c.tran_service_code)
                    VALUES   (                             --src.core_sn1,
                              TRIM (src.tmresv07) || seq_core_sn.NEXTVAL,
                              --TRIM(src.tmdorc) || TRIM(src.tmtellid) || TRIM(src.tmtxseq) ||
                              --TRIM(src.tmentdt7),
                              src.tran_date,
                              src.post_date,
                              src.tmdorc,
                              src.tmtxamt,
                              src.tmglcur,
                              src.tmorgamt,
                              --src.channel_id,
                              /*DECODE(src.staff_name,
                              NULL,
                              'CNT',
                              'IBS')*/
                              DECODE (src.sender_id,
                                      NULL, 'CNT',
                                      src.sender_id),
                              src.tmefth,
                              (case when length(src.tmacctno) = 13 then LPAD (src.tmacctno, 14, '0') else TO_CHAR(src.tmacctno) end),
                              src.tminsdate,
                              src.tmtellid,
                              src.tmtxseq,
                              --'0',
                              DECODE (src.tmapptype,
                                      'D', '6',
                                      'T', '7',
                                      'L', '8',
                                      'S', '6',
                                      '0'),
                              'SUCC' /* DECODE(src.tmtxstat,
                                     '',
                                     'FAIL',
                                     'PP',
                                     'PEND',
                                     'PT',
                                     'SUCC',
                                     'SUCC') */
                                    ,
                              'SYNC',
                              src.tran_sn,
                              DECODE (src.tran_service_code,
                                      NULL, 'RTR001',
                                      'RTR002'));
    
            COMMIT;
    
            
--            MERGE INTO   bc_transfer_history c
--                 USING   (SELECT   b.tran_sn,
--                                   TRIM (a.tmtxstat) AS tmtxstat,
--                                   a.tmentdt7,
--                                   a.tmresv07,
--                                   a.tmacctno
--                            FROM   sync_tmtrans a, sync_tranmap b
--                           WHERE   TRIM (a.tmtellid) = b.teller_id(+)
--                                   AND TRIM (a.tmtxseq) =
--                                          b.host_tran_sn(+)
--                                   AND TRUNC(TO_DATE (a.tmentdt7,
--                                                      'yyyyddd')) =
--                                          TRUNC(TO_DATE (
--                                                    b.host_real_date(+),
--                                                    'yyyyddd'))
--                                   AND LPAD (a.tmacctno, 14, '0') IN
--                                              (SELECT   x.acct_no
--                                                 FROM   sync_account_info x)
--                                  -- AND a.tmtiment BETWEEN g_min_time
--                                  --                    AND  g_max_time 
--                                                      /*AND    TRUNC(TO_DATE(a.tmresv07, 'yyyyMMddHH24miss')) =
--                                                                                                  TRUNC(SYSDATE)*/
--                                                                     --AND    TRIM(a.tmtxstat) = 'CE' --giao dich huy
--                                                                     -- AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
--                         ) src
--                    ON   (c.tran_sn = src.tran_sn
--                          AND c.rollout_account_no =
--                                 LPAD (src.tmacctno, 14, '0'))
--            WHEN MATCHED
--            THEN
--                UPDATE SET
--                    c.status = DECODE (src.tmtxstat, 'CE', 'FAIL', 'SUCC');
--            COMMIT;
       
    UPDATE sync_cif
       SET status = 'Y'
     WHERE cif_no IN (SELECT cif_no FROM sync_cif_n scn1)
       AND "TYPE" = 'TM';
  
    COMMIT;
  END;
END PK_SYNC_DATA;

/
