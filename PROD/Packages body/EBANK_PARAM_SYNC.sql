--------------------------------------------------------
--  DDL for Package Body EBANK_PARAM_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."EBANK_PARAM_SYNC" IS
  v_start_date DATE;

  g_error_level NUMBER;

  g_loop_count NUMBER;

  g_limit_count NUMBER;

  g_min_count NUMBER;

  g_max_count NUMBER;

  g_cif_count NUMBER;

  g_error_count NUMBER;
  ----------------------------------------------------------------------------
  --Date: 11/05/2011
  --Comment: Sync foreign exchange rate
  --ManhNV
  PROCEDURE proc_fx_rate_param_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
--    INSERT INTO sync_ssfxrtl1
--      SELECT
--      /*+ ALL_ROWS */
--       jfxcod,
--       jfxdsc,
--       jfxbrt,
--       jfxsrt,
--       jfxmrt,
--       jfxart,
--       jfxbkc,
--       jfxcno
--      FROM   SVPARPV51.ssfxrtl1@DBLINK_DATA;
    /*
    TODO: owner="LeDucAnh" category="Fix" priority="2 - Medium" created="18/08/2011"
    text="Fix database link ch?? hoa - ch?? th???ng, thi?nh thoa?ng hay bi?"
    */
    MERGE INTO bk_fx_rate c
    USING (SELECT
           /*+ INDEX(sync_ssfxrtl1, IDX_SYNC_SSFXRTL1)*/
            TRIM(jfxcod) jfxcod,
            TRIM(jfxdsc) jfxdsc,
            jfxbrt,
            jfxsrt,
            jfxmrt,
            jfxart,
            TRIM(jfxbkc) jfxbkc,
            jfxcno
           FROM SYNC_SSFXRT) a
    ON (a.jfxcod = TRIM(c.receipt_ccy))
    WHEN MATCHED THEN
      UPDATE
      SET    c.ccy_buy_rate  = a.jfxbrt,
             c.ccy_sell_rate = a.jfxsrt,
             c.ccy_mid_rate  = a.jfxmrt,
             c.ccy_avg_rate  = a.jfxart,
             c.sell_ccy      = a.jfxbkc
    WHEN NOT MATCHED THEN
      INSERT
        (c.receipt_ccy,
         c.ccy_buy_rate,
         c.ccy_sell_rate,
         c.ccy_mid_rate,
         c.ccy_avg_rate,
         c.sell_ccy)
      VALUES
        (a.jfxcod,
         a.jfxbrt,
         a.jfxsrt,
         a.jfxmrt,
         a.jfxart,
         a.jfxbkc);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
  
    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_fx_rate_param_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_fx_rate_param_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  --Date: 11/05/2011
  --Comment: Sync ID type
  --ManhNV
  PROCEDURE proc_id_type_param_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    DELETE FROM bk_cert_type;
  
    INSERT INTO bk_cert_type
      (cert_type,
       description,
       country_code)
      (SELECT TRIM(cfidcd),
              TRIM(cfidsc),
              TRIM(cfidct)
       FROM   SVPARPV51.cfiddf@dblink_data);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
  
    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_id_type_param_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_id_type_param_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  --Date: 11/05/2011
  --Comment: Sync product
  --ManhNV
  PROCEDURE proc_acct_product_param_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    DELETE bk_acct_product_type;
    --DELETE bk_receipt_product;
  
    INSERT INTO bk_acct_product_type
      (product_type,
       ccy_code,
       description,
       acc_type)
      (SELECT TRIM(sccode),
              TRIM(dp2cur),
              TRIM(pscdes),
              'DD'
       FROM   SVPARPV51.ddpar2@dblink_data);
  
    /* INSERT INTO bk_acct_product_type
    (
      product_type,
      description,
      short_name,
      group_code,
      group_description,
      acc_type,
      ccy_code
    )(
         SELECT
             TRIM(ptype),
             TRIM(pdesc),
             TRIM(psdsc),
             pgroup,
             TRIM(pgrpds),
             'CD',
             TRIM(pcurty)
         FROM
             SVPARPV51.cdpar2@dblink_data
     ); */
  
    /* INSERT INTO bk_receipt_product
    (product_code,
     product_name,
     currency_code,
     
     status)
    (SELECT TRIM(ptype),
            TRIM(pdesc),
            TRIM(pcurty),
            'ACTV' --active
     FROM   SVPARPV51.cdpar2@dblink_data); */
  
    INSERT INTO bk_acct_product_type
      (product_type,
       description,
       group_code,
       group_description,
       ccy_code,
       acc_type)
      (SELECT TRIM(ptype),
              TRIM(ptydsc),
              plngrp,
              TRIM(pgrdsc),
              TRIM(pcurty),
              'LN'
       FROM   SVPARPV51.lnpar2@dblink_data);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
  
    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_acct_product_param_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_acct_product_param_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  --Date: 11/05/2011
  --Comment: sync region info
  --ManhNV
  PROCEDURE proc_region_param_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    --do delete and insert parent region code
    DELETE FROM bk_region a
    WHERE  a.region_code = 'VN';
  
    INSERT INTO bk_region
      (region_code,
       p_region_code,
       region_name,
       region_level,
       description)
    VALUES
      ('VN',
       '-1',
       'VIET NAM',
       1,
       'VIET NAM');
  
    MERGE INTO bk_region c
    USING (SELECT v.prov_code,
                  fullname
           FROM   gwservice.area@DBLINK_GW v) a
    ON (a.prov_code = TRIM(c.region_code))
    WHEN MATCHED THEN
      UPDATE
      SET    c.region_name  = a.fullname,
             c.description  = a.fullname,
             c.region_level = 2
    WHEN NOT MATCHED THEN
      INSERT
        (c.p_region_code,
         c.region_code,
         c.region_name,
         c.description,
         c.region_level)
      VALUES
        ('VN',
         a.prov_code,
         a.fullname,
         a.fullname,
         2);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link DBLINK_GW';
  
    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_region_param_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_region_param_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  --Date: 11/05/2011
  --Comment: Sync currency
  PROCEDURE proc_currucy_param_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    INSERT INTO sync_currucy
      SELECT a.jfxdsc,
             a.jfxcod
      FROM   SVPARPV51.SSFXRT@DBLINK_DATA a;
  
    MERGE INTO bk_currency c
    USING (SELECT TRIM(jfxcod) jfxcod,
                  TRIM(jfxdsc) jfxdsc
           FROM   sync_currucy) a
    ON (a.jfxcod = TRIM(c.currency_code))
    WHEN MATCHED THEN
      UPDATE
      SET    c.name        = a.jfxdsc,
             c.description = a.jfxdsc,
             c.update_time = SYSDATE
    WHEN NOT MATCHED THEN
      INSERT
        (c.currency_code,
         c.name,
         c.description,
         c.create_time,
         c.update_time)
      VALUES
        (a.jfxcod,
         a.jfxdsc,
         a.jfxdsc,
         SYSDATE,
         SYSDATE);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';
  
    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_currucy_param_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_currucy_param_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  --Date: 11/05/2011
  --Comment: Sync bussiness
  PROCEDURE proc_bussiness_type_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    INSERT INTO sync_buss_type
      SELECT a.vcesec,
             a.vcdesc,
             a.VCSORD
      FROM   SVPARPV51.BVPARC@dblink_data a;
  
    MERGE INTO bk_buss_type c
    USING (SELECT TRIM(vcesec) vcesec,
                  TRIM(vcdesc) vcdesc
           FROM   sync_buss_type) a
    ON (c.buss_type = TRIM(a.vcesec))
    WHEN MATCHED THEN
      UPDATE
      SET    --c.buss_type   = a.vcesec,
             c.description = a.vcdesc
      
      
      
      
      
      
    
    WHEN NOT MATCHED THEN
      INSERT
        (c.buss_type,
         c.description)
      VALUES
        (a.vcesec,
         a.vcdesc);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
  
    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_buss_type_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_buss_type_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  ----------------------------------------------------------------------------
  --Date: 11/05/2011
  --Comment: Sync officer
  PROCEDURE proc_officer_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    INSERT INTO sync_officer
      SELECT TRIM(a.ssooff) ssooff,
             TRIM(a.ssobrn) ssobrn,
             TRIM(a.ssoidn) ssoidn,
             TRIM(a.ssonam) ssonam,
             TRIM(a.ssosna) ssosna,
             TRIM(a.ssocur) ssocur,
             TRIM(a.ssoalg) ssoalg,
             TRIM(a.ssoalm) ssoalm,
             TRIM(a.sscntr) sscntr,
             TRIM(a.ssdept) ssdept
      FROM   SVPARPV51.SSOFFR@dblink_data a;
  
    MERGE INTO bk_officer a
    USING (SELECT TRIM(b.ssooff) ssooff,
                  TRIM(b.ssobrn) ssobrn,
                  TRIM(b.ssoidn) ssoidn,
                  TRIM(b.ssonam) ssonam,
                  TRIM(b.ssosna) ssosna,
                  TRIM(b.ssocur) ssocur,
                  TRIM(b.ssoalg) ssoalg,
                  TRIM(b.ssoalm) ssoalm,
                  TRIM(b.sscntr) sscntr,
                  TRIM(b.ssdept) ssdept
           FROM   sync_officer b) src
    ON (TRIM(a.officer_code) = TRIM(src.ssooff))
    WHEN MATCHED THEN
      UPDATE
      SET    a.branch_code      = LPAD(src.ssobrn,
                                       3,
                                       '0'),
             a.officer_id       = src.ssoidn,
             a.officer_name     = src.ssonam,
             a.short_name       = src.ssosna,
             a.currency_code    = src.ssocur,
             a.apro_limit_group = src.ssoalg,
             a.apro_limit       = src.ssoalm,
             a.cost_center      = src.sscntr,
             a.deparment        = src.ssdept
    WHEN NOT MATCHED THEN
      INSERT
        (a.officer_code,
         a.branch_code,
         a.officer_id,
         a.officer_name,
         a.short_name,
         a.currency_code,
         a.apro_limit_group,
         a.apro_limit,
         a.cost_center,
         a.deparment)
      VALUES
        (src.ssooff,
         LPAD(src.ssobrn,
              3,
              '0'),
         src.ssoidn,
         src.ssonam,
         src.ssosna,
         src.ssocur,
         src.ssoalg,
         src.ssoalm,
         src.sscntr,
         src.ssdept);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
  
    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_officer_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_officer_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;


  PROCEDURE proc_fee_discount_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    g_loop_count  := 10;
    g_limit_count := 200000;
    g_min_count   := 0;
    g_max_count   := 0;
    g_cif_count   := 0;
    g_error_count := 0;
  
    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';
  
    g_error_level := 1;
    --FOR i IN 1 .. g_loop_count
    --LOOP
    -- try g_loop_count times
    --BEGIN
    SAVEPOINT s_insert_temp;
    LOOP
      BEGIN
        g_min_count := g_max_count;
        g_max_count := g_min_count + g_limit_count;
        INSERT INTO sync_cfmast_fee
          SELECT /*+ ALL_ROWS */
           a.cfcifn,
           a.cfsnme,
           a.cfbust
          FROM   svdatpv51.cfmast@DBLINK_DATA a
          WHERE  a.cfcifn BETWEEN g_min_count AND g_max_count;
      
        COMMIT;
        --khong co them ban ghi nao              
        EXIT WHEN(g_max_count > g_cif_count);
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_temp;
          g_min_count   := 0;
          g_error_count := g_error_count + 1;
          IF (g_error_count >= 10)
          THEN
            RETURN;
          END IF;
          --DBMS_LOCK.SLEEP(10);                            
      END;
    END LOOP;
  
    g_error_level := 2;
    g_min_count   := 0;
    g_max_count   := 0;
    SAVEPOINT s_insert_cffor_temp;
    LOOP
      BEGIN
        g_min_count := g_max_count;
        g_max_count := g_min_count + g_limit_count;
      
        INSERT INTO sync_cfoffl
          SELECT /*+ ALL_ROWS */
           a.cfaccn,
           cfatyp,
           cfoffr
          FROM   svdatpv51.cfoffl@DBLINK_DATA a
          WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;
      
        COMMIT;
        --khong co them ban ghi nao              
        EXIT WHEN(g_max_count > g_cif_count);
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_cffor_temp;
          g_min_count   := 0;
          g_error_count := g_error_count + 1;
          IF (g_error_count >= 10)
          THEN
            RETURN;
          END IF;
          --DBMS_LOCK.SLEEP(10);                            
      END;
    END LOOP;
  
    g_error_level := 3;
    MERGE INTO bk_fee_discount c
    USING (SELECT DISTINCT TRIM(a.cfcifn) AS cfcifn,
                           TRIM(a.cfsnme) AS cfsnme,
                           TRIM(a.cfbust) AS cfbust,
                           TRIM(b.cfoffr) AS cfoffr
           FROM   sync_cfmast_fee a,
                  (SELECT c.cfoffr,
                          c.cfaccn
                   FROM   sync_cfoffl c
                   WHERE  c.cfoffr = (SELECT MAX(d.cfoffr)
                                      FROM   sync_cfoffl d
                                      WHERE  d.cfaccn = c.cfaccn)) b
           WHERE  a.cfcifn = b.cfaccn(+)
           AND    TRIM(a.cfcifn) IS NOT NULL
           AND    a.cfcifn <> 0) src
    ON (c.cif_no = src.cfcifn)
    WHEN MATCHED THEN
      UPDATE
      SET    c.cif_name      = src.cfsnme,
             c.business_type = src.cfbust,
             c.office_code   = src.cfoffr
      --c.modified_date = SYSDATE      
      
      
      
      
      
    
    WHEN NOT MATCHED THEN
      INSERT
        (c.cif_no,
         c.cif_name,
         c.business_type,
         c.office_code)
      VALUES
        (src.cfcifn,
         src.cfsnme,
         src.cfbust,
         src.cfoffr);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
  
    /*EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
    SYSDATE,
    'proc_fee_discount_sync',
    NULL,
    'SUCC');*/
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_fee_discount_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               SQLERRM,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               1,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;


  PROCEDURE proc_fee_discount_onday_sync IS
  BEGIN
    v_start_date := SYSDATE;
  
    g_loop_count  := 10;
    g_limit_count := 200000;
    g_min_count   := 0;
    g_max_count   := 0;
    g_cif_count   := 0;
    g_error_count := 0;
  
    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';
  
    g_error_level := 1;
  
  
    SAVEPOINT s_insert_cfmast_temp;
  
    LOOP
      BEGIN
        g_min_count := g_max_count;
        g_max_count := g_min_count + g_limit_count;
        INSERT INTO sync_cfmast_fee
          SELECT /*+ ALL_ROWS */
           a.cfcifn,
           a.cfsnme,
           a.cfbust
          FROM   svdatpv51.cftnew@dblink_data a
          WHERE  a.cfcifn BETWEEN g_min_count AND g_max_count;
      
        COMMIT;
        --khong co them ban ghi nao
        EXIT WHEN(g_max_count > g_cif_count);
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_cfmast_temp;
          g_min_count   := 0;
          g_error_count := g_error_count + 1;
        
          IF (g_error_count >= 10)
          THEN
            RETURN;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;
  
  
    g_error_level := 2;
    g_min_count   := 0;
    g_max_count   := 0;
  
    SAVEPOINT s_insert_cffor_temp;
  
    LOOP
      BEGIN
        g_min_count := g_max_count;
        g_max_count := g_min_count + g_limit_count;
      
        INSERT INTO sync_cfoffl
          SELECT /*+ ALL_ROWS */
           a.cfaccn,
           cfatyp,
           cfoffr
          FROM   svdatpv51.cfoffl@dblink_data a
          WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;
      
        COMMIT;
        --khong co them ban ghi nao
        EXIT WHEN(g_max_count > g_cif_count);
      EXCEPTION
        WHEN OTHERS THEN
          ROLLBACK TO s_insert_cffor_temp;
          g_min_count   := 0;
          g_error_count := g_error_count + 1;
        
          IF (g_error_count >= 10)
          THEN
            RETURN;
          END IF;
          --DBMS_LOCK.SLEEP(10);
      END;
    END LOOP;
  
    g_error_level := 3;
  
    MERGE INTO bk_fee_discount c
    USING (SELECT DISTINCT TRIM(a.cfcifn) AS cfcifn,
                           TRIM(a.cfsnme) AS cfsnme,
                           TRIM(a.cfbust) AS cfbust,
                           TRIM(b.cfoffr) AS cfoffr
           FROM   sync_cfmast_fee a,
                  (SELECT c.cfoffr,
                          c.cfaccn
                   FROM   sync_cfoffl c
                   WHERE  c.cfoffr = (SELECT MAX(d.cfoffr)
                                      FROM   sync_cfoffl d
                                      WHERE  d.cfaccn = c.cfaccn)) b
           WHERE  a.cfcifn = b.cfaccn(+)
           AND    TRIM(a.cfcifn) IS NOT NULL
           AND    a.cfcifn <> 0) src
    ON (c.cif_no = src.cfcifn)
    WHEN MATCHED THEN
      UPDATE
      SET    c.cif_name      = src.cfsnme,
             c.business_type = src.cfbust,
             c.office_code   = src.cfoffr
      --c.modified_date = SYSDATE
      
      
      
      
      
    
    WHEN NOT MATCHED THEN
      INSERT
        (c.cif_no,
         c.cif_name,
         c.business_type,
         c.office_code)
      VALUES
        (src.cfcifn,
         src.cfsnme,
         src.cfbust,
         src.cfoffr);
  
    COMMIT;
  
    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
  
    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_fee_discount_onday_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_fee_discount_onday_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
    
  END;

----------------------------------------------------------------------------
END;

/
