--------------------------------------------------------
--  DDL for Package Body EBANK_CARD_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."EBANK_CARD_SYNC" 
/* Formatted on 20-Jun-2011 16:17:37 (QP5 v5.126) */
 IS

  v_start_date DATE;

  g_error_level NUMBER;

  g_loop_count NUMBER;

  g_limit_count NUMBER;

  g_min_count NUMBER;

  g_max_count NUMBER;

  g_cif_count NUMBER;

  g_error_count NUMBER;

  g_error_sub_count NUMBER;

  PROCEDURE proc_card_info_sync IS
    v_start_date      DATE;
    v_checkpoint_date NUMBER;
  BEGIN
    v_start_date      := SYSDATE;
    g_loop_count      := 10;
    g_limit_count     := 200000;
    g_min_count       := 0;
    g_max_count       := 0;
    g_cif_count       := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
  
    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';
  
    SELECT to_number(to_char(SYSDATE,
                             'yyyy') || to_char(SYSDATE,
                                                'mm') ||
                     to_char(SYSDATE,
                             'dd'))
    INTO   v_checkpoint_date
    FROM   dual;
  
    g_cif_count := g_cif_count + 1000;
  
    g_error_level := 1;
    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;
      
        LOOP
          BEGIN
            SAVEPOINT s_intert_temp_l;
          
            g_min_count := g_max_count;
            g_max_count := g_min_count + g_limit_count;
          
            INSERT INTO sync_card_info
              SELECT
              /*+ ALL_ROWS */
               a.eccard, --Card Number
               a.eccsts, --Card Status
               a.ecdtef, --Effective Date
               a.ecdex4, --Expire Date
               a.ecctyp, --Card Type
               a.ecacn1, --  Account 1
               a.ECATY1, --  Account Type 1
               a.eccat1,
               a.eccur1, --currency code
               a.ecacn2, --  Account 2
               a.ECATY2, --  Account Type 2
               a.eccat2,
               a.eccur2,
               a.ecacn3, --  Account 3
               a.ECATY3, --  Account Type 3
               a.eccat3,
               a.eccur3,
               a.ecacn4, --  Account 4
               a.ECATY4, --  Account Type 4
               a.eccat4,
               a.eccur4,
               a.ecacn5, --  Account 5
               a.ECATY5, --  Account Type 5
               a.eccat5,
               a.eccur5,
               a.ecacn6, --  Account 6
               a.ECATY6, --  Account Type 6
               a.eccat6,
               a.eccur6,
               a.ecacn7, --  Account 7
               a.ECATY7, --  Account Type 7
               a.eccat7,
               a.eccur7,
               a.ecacn8, --  Account 8
               a.ECATY8, --  Account Type 8
               a.eccat8,
               a.eccur8,
               a.ecacn9, --  Account 9
               a.ECATY9, --  Account Type 9
               a.eccat9,
               a.eccur9,
               a.ecac10, --  Account 10
               a.ECAT10, --  Account Type 10
               a.ecct10,
               a.eccu10,
               a.ecac11, --  Account 11
               a.ECAT11, --  Account Type 11
               a.ecct11,
               a.eccu11,
               a.ecac12, --  Account 12
               a.ECAT12, --  Account Type 12
               a.ecct12,
               a.eccu12,
               a.ecac13, --  Account 13
               a.ECAT13, --  Account Type 13
               a.ecct13,
               a.eccu13,
               a.ecac14, --  Account 14
               a.ECAT14, --  Account Type 14
               a.ecct14,
               a.eccu14,
               a.ecac15, --  Account 15
               a.ECAT15, --  Account Type 15
               a.ecct15,
               a.eccu15,
               a.ecac16, --  Account 16
               a.ECAT16, --  Account Type 16
               a.ecct16,
               a.eccu16,
               a.ecac17, --  Account 17
               a.ECAT17, --  Account Type 17
               a.ecct17,
               a.eccu17,
               a.ecac18, --  Account 18
               a.ECAT18, --  Account Type 18
               a.ecct18,
               a.eccu18,
               a.ecdtis, --  Issue Date
               a.eccif, --  CIF Number
               --a.ECCSN1, --  Customer Name 1
               --a.ECADD1, --  Address Line 1
               a.eccrty, --  Credit Card Type
               --a.ECDTTN, --  Last Transaction Date
               --a.ECTMTN--  Last Transaction Time
               a.ecshnm --holder
              FROM   SVDATPEBS.ebcrdm@dblink_data1 a
              WHERE  (a.ecdtad >= v_checkpoint_date OR
                     a.ecdstc >= v_checkpoint_date OR
                     a.ecdtis >= v_checkpoint_date OR
										 a.ecdtef >= v_checkpoint_date)
                    
                    /*(
                      TRUNC(TO_DATE(a.ecdtis,'yyyyddd')) >= TRUNC(SYSDATE) OR 
                      TRUNC(TO_DATE(a.ecdtef,'yyyyddd')) >= TRUNC(SYSDATE)
                    )*/
                    --  product SVDATPEBS.ebcrdm@dblink_data a
                    --SVDATPEBS.ebcrdm@dblink_data a;
                    /*  WHERE
                    a.eccif IS NOT NULL
                    AND a.eccard IS NOT NULL
                    AND a.eccsts IS NOT NULL
                    AND a.ecctyp IS NOT NULL; */
                    
                    /* 1207910000891118                                            
                    1207910000083724                                            
                    1207910000674910                                            
                    1207910000739515 */
                    AND  a.ECCTYP != 'FCBP' 
              AND    TO_NUMBER(a.eccif) BETWEEN g_min_count AND g_max_count;
          
            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;
              IF (g_error_sub_count >= 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;
        EXIT;
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_error_count := g_error_count + 1;
          g_min_count   := 0;
          g_max_count   := 0;
          IF (g_error_count >= 10)
          THEN
            RAISE;
          END IF;
          --DBMS_LOCK.SLEEP(30);
      END;
    END LOOP;
  
    --tam thoi xoa bo nhung thang join the
    --DELETE FROM sync_card_info WHERE eccard IN(1207910000891118,1207910000083724,1207910000674910,1207910000739515);
  
    /* DELETE FROM sync_card_info
    WHERE  eccard IN (SELECT eccard
                      FROM   sync_card_info
                      GROUP  BY eccard
                      HAVING COUNT(1) > 1); */
    g_error_level := 2;
    INSERT INTO sync_card_info_t
      SELECT *
      FROM   sync_card_info sci
      WHERE  sci.eccard NOT IN (SELECT sci2.eccard
                                FROM   sync_card_info sci2
                                GROUP  BY sci2.eccard
                                HAVING COUNT(1) > 1)
      AND    TRIM(sci.eccif) IN
             (SELECT LPAD(bui.cif_no,
                           19,
                           '0')
               FROM   bc_user_info bui)
      --AND    sci.eccsts <> 'DELT'
      AND    TRIM(sci.eccrty) IS NULL
      AND    sci.eccif IS NOT NULL
      AND    sci.eccard IS NOT NULL
      AND    sci.eccsts IS NOT NULL;
  
    /*g_error_level := 3;
    SELECT
    \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   min_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            FROM   sync_card_info_t
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 1
    ORDER  BY 1;    
    
    g_error_level := 4;
    SELECT
    \*+ ALL_ROWS *\
     qry.rid BULK COLLECT
    INTO   max_rid_tab
    FROM   (SELECT MOD(ROWNUM,
                       1000) rnm,
                   ROWID rid
            FROM   sync_card_info_t
            ORDER  BY ROWID) qry
    WHERE  qry.rnm = 0
    UNION
    SELECT
    \*+ ALL_ROWS *\
     MAX(ROWID)
    FROM   sync_card_info_t;*/
  
    g_error_level := 5;
    --sync bk_card_info
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_info eb_card
    USING (SELECT
           /*+ ALL_ROWS */
            TRIM(gt_card.eccard) card_no,
            TRIM(gt_card.eccsts) status,
            'DB' card_type,
            LTRIM(TRIM(gt_card.eccif),
                  0) cif_no,
            TRIM(gt_card.ecdtef) active_date,
            TRIM(gt_card.ecdex4) expiry_date,
            TRIM(gt_card.ecdtis) issued_date,
            TRIM(gt_card.ecshnm) ecshnm
           FROM   sync_card_info_t gt_card
           --WHERE  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --WHERE  gt_card.eccif IS NOT NULL
           --AND    gt_card.eccard IS NOT NULL
           --AND    gt_card.eccsts IS NOT NULL
           /* AND    EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) */
           --AND gt_card.eccif IN(SELECT cif_no FROM bc_user_info bui)
           --AND    gt_card.eccsts <> 'DELT'
           --AND    TRIM(gt_card.eccrty) IS NULL
           --AND LENGTH(gt_card.eccard) > 0
           --AND LENGTH(gt_card.eccif) > 0
           --AND LENGTH(gt_card.ecctyp) > 0
           --AND LENGTH(gt_card.eccsts) > 0
           --AND TRIM(gt_card.eccard) <> '9704260001756113'
           --AND TRIM(gt_card.eccif) <> '0000000000000425240'
           ) src
    ON (TRIM(eb_card.card_no) = TRIM(src.card_no)
    --AND TRIM(eb_card.cif_no) = TRIM(src.cif_no)
    --AND TRIM(eb_card."TYPE") = src.card_type
    )
    WHEN MATCHED THEN
      UPDATE
      SET    eb_card.status      = src.status,
             eb_card.app_date    = DECODE((LENGTH(src.active_date)),
                                          8,
                                          TO_DATE(TO_CHAR(src.active_date),
                                                  'YYYYMMDD'),
                                          NULL),
             eb_card.issued_date = DECODE((LENGTH(src.issued_date)),
                                          8,
                                          TO_DATE(TO_CHAR(src.issued_date),
                                                  'YYYYMMDD'),
                                          NULL),
             eb_card.holder      = src.ecshnm,
             eb_card.type        = 'DB'
      
      
      
      
    
    WHEN NOT MATCHED THEN
      INSERT
        (eb_card.card_no,
         eb_card.status,
         eb_card.TYPE,
         eb_card.cif_no,
         eb_card.app_date,
         eb_card.issued_date,
         eb_card.holder)
      VALUES
        (src.card_no,
         src.status,
         --src.card_type,
         'DB', --debit
         src.cif_no,
         DECODE((LENGTH(src.active_date)),
                8,
                TO_DATE(TO_CHAR(src.active_date),
                        'YYYYMMDD'),
                NULL),
         DECODE((LENGTH(src.issued_date)),
                8,
                TO_DATE(TO_CHAR(src.issued_date),
                        'YYYYMMDD'),
                NULL),
         src.ecshnm);
  
    COMMIT;
  
    /* MERGE INTO bk_ccard_info eb_ccard
    USING (
    SELECT
    TRIM(gt_card.ecdtef) active_date,
    TRIM(gt_card.ecdex4) expiry_date,
    TRIM(gt_card.ecdtis) issued_date,
    LTRIM(TRIM(gt_card.eccif), 0) cif_no,
    TRIM(gt_card.eccard) credit_card_no,
    TRIM(gt_card.ECCRTY) credit_card_type,
    TRIM(gt_card.eccsts) status
    FROM
    sync_card_info gt_card
    WHERE
    EXISTS(
    SELECT
    bc.cif_no
    FROM
    bk_cif bc
    WHERE
    bc.cif_no = LTRIM(TRIM(gt_card.eccif), 0)
    )
    AND TRIM(gt_card.eccsts) <> 'DELT'
    AND LENGTH(TRIM(gt_card.eccrty)) > 0
    --AND currency_code not null
    ) src
    ON
    (
    TRIM(eb_ccard.cif_no) = src.cif_no
    AND TRIM(eb_ccard.credit_card_no) = src.credit_card_no
    --AND eb_ccard.status = src.status--chu y sau xem lai
    )
    WHEN MATCHED
    THEN
    UPDATE
    SET
    eb_ccard.active_date = DECODE(
    (LENGTH(src.active_date)),
    8,
    TO_DATE(TO_CHAR(src.active_date), 'YYMMDD'),
    NULL
    ),
    eb_ccard.expiry_date = DECODE(
    (LENGTH(src.issued_date)),
    8,
    ADD_MONTHS(TO_DATE(TO_CHAR(src.issued_date), 'YYMMDD'), 24),
    NULL
    ),
    eb_ccard.issued_date = DECODE(
    (LENGTH(src.issued_date)),
    8,
    TO_DATE(TO_CHAR(src.issued_date), 'YYMMDD'),
    NULL
    ),
    eb_ccard.card_type = src.credit_card_type
    WHEN NOT MATCHED
    THEN
    INSERT 
    (
    eb_ccard.cif_no,
    eb_ccard.credit_card_no,
    eb_ccard.active_date,
    eb_ccard.expiry_date,
    eb_ccard.issued_date,
    eb_ccard.card_type,
    eb_ccard.currency_code,
    eb_ccard.p_credit_card_no
    )
    VALUES
    (
    src.cif_no,
    src.credit_card_no,
    DECODE(
    (LENGTH(src.active_date)),
    8,
    TO_DATE(TO_CHAR(src.active_date), 'YYMMDD'),
    NULL
    ),
    DECODE(
    (LENGTH(src.issued_date)),
    8,
    ADD_MONTHS(TO_DATE(TO_CHAR(src.issued_date), 'YYMMDD'), 24),
    NULL
    ),
    DECODE(
    (LENGTH(src.issued_date)),
    8,
    TO_DATE(TO_CHAR(src.issued_date), 'YYMMDD'),
    NULL
    ),
    src.credit_card_type,
    'VND',    --cai nay chiu
    SUBSTR(src.credit_card_no, 0, 4)
    ); */
  
    --sync bk_card_account
    --1. Delete table bk_card_account
    --DELETE FROM bk_card_account;
  
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    --2. Insert into bk_card_account
    g_error_level := 6;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    /*MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn1) AS acct_no1,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat1) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           WHERE  LENGTH(TRIM(gt_card.ecacn1)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccrty) IS NULL
           --AND TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no1)
    WHEN NOT MATCHED THEN
    --acc_no 1
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (src.card_no,
         src.acct_no1,
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    
    g_error_level := 7;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn2) AS acct_no2,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat2) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           WHERE  LENGTH(TRIM(gt_card.ecacn2)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no2)
    WHEN NOT MATCHED THEN
    --acc_no 2
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no2),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 8;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn3) AS acct_no3,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat3) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           -- 0))
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           WHERE  LENGTH(TRIM(gt_card.ecacn3)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no3)
    WHEN NOT MATCHED THEN
    --acc_no 3
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no3),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 9;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn4) AS acct_no4,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat4) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           WHERE  LENGTH(TRIM(gt_card.ecacn4)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no4)
    WHEN NOT MATCHED THEN
    --acc_no 4
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no4),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 10;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn5) AS acct_no5,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat5) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           WHERE  LENGTH(TRIM(gt_card.ecacn5)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no5)
    WHEN NOT MATCHED THEN
    --acc_no 5
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no5),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    
    g_error_level := 11;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn6) AS acct_no6,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat6) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           WHERE  LENGTH(TRIM(gt_card.ecacn6)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no6)
    WHEN NOT MATCHED THEN
    --acc_no 6
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no6),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 12;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn7) AS acct_no7,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat7) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           WHERE  LENGTH(TRIM(gt_card.ecacn7)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no7)
    WHEN NOT MATCHED THEN
    --acc_no 7
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no7),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 13;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn8) AS acct_no8,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat8) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           WHERE  LENGTH(TRIM(gt_card.ecacn8)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no8)
    WHEN NOT MATCHED THEN
    --acc_no 8
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no8),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 14;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecacn9) AS acct_no9,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.eccat9) AS is_default
           FROM   sync_card_info_t gt_card
           --WHERE  EXISTS (SELECT bc.cif_no
           --        FROM   bk_cif bc
           --        WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
           --                                 0))
           WHERE  LENGTH(TRIM(gt_card.ecacn9)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no9)
    WHEN NOT MATCHED THEN
    --acc_no 9
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no9),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 15;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac10) AS acct_no10,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat10) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac10)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no10)
    WHEN NOT MATCHED THEN
    --acc_no 10
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no10),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 15;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac11) AS acct_no11,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat11) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac11)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no11)
    WHEN NOT MATCHED THEN
    --acc_no 11
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no11),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 17;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac12) AS acct_no12,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat12) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac12)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no12)
    WHEN NOT MATCHED THEN
    --acc_no 12
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no12),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 18;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac13) AS acct_no13,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat13) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac13)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no13)
    WHEN NOT MATCHED THEN
    --acc_no 13
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no13),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 19;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac14) AS acct_no14,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat14) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac14)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no14)
    WHEN NOT MATCHED THEN
    --acc_no 14
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no14),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 20;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac15) AS acct_no15,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat15) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac15)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no15)
    WHEN NOT MATCHED THEN
    --acc_no 15
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no15),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 21;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac16) AS acct_no16,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat16) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac16)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no16)
    WHEN NOT MATCHED THEN
    --acc_no 16
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no16),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 22;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac17) AS acct_no17,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat17) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac17)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no17)
    WHEN NOT MATCHED THEN
    --acc_no 17
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no17),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');
    g_error_level := 23;
    --FORALL i IN min_rid_tab.FIRST .. min_rid_tab.LAST
    MERGE INTO bk_card_account eb_target
    USING (SELECT TRIM(gt_card.eccard) AS card_no,
                  TRIM(gt_card.ecac18) AS acct_no18,
                  --IS_DEFAULT o dau
                  TRIM(gt_card.ecat18) AS is_default
           FROM   sync_card_info_t gt_card
           \* WHERE  EXISTS (SELECT bc.cif_no
           FROM   bk_cif bc
           WHERE  bc.cif_no = LTRIM(TRIM(gt_card.eccif),
                                    0)) *\
           WHERE  LENGTH(TRIM(gt_card.ecac18)) > 1
           --AND  gt_card.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
           --AND    TRIM(gt_card.eccsts) <> 'DELT'
           ) src
    ON (eb_target.card_no = src.card_no AND eb_target.acct_no = src.acct_no18)
    WHEN NOT MATCHED THEN
    --acc_no 18
      INSERT
        (card_no,
         acct_no,
         is_default)
      VALUES
        (TRIM(src.card_no),
         TRIM(src.acct_no18),
         DECODE(src.is_default,
                'F',
                'Y',
                'N'))
    WHEN MATCHED THEN
      UPDATE
      SET    eb_target.is_default = DECODE(src.is_default,
                                           'F',
                                           'Y',
                                           'N');*/
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
  
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_card_info_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_card_info_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /* DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255)); */
  END;
END;

/
