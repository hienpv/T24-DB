--------------------------------------------------------
--  DDL for Package Body CSPKG_TRANSACTION_ETL_SYNC_2
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."CSPKG_TRANSACTION_ETL_SYNC_2" 
IS
 /*----------------------------------------------------------------------------------------------------
     ** Module   : COMMODITY SYSTEM
     ** and is copyrighted by FSS.
     **
     **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
     **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
     **    graphic, optic recording or otherwise, translated in any language or computer language,
     **    without the prior written permission of Financial Software Solutions. JSC.
     **
     **  MODIFICATION HISTORY
     **  Person      Date           Comments
     **  LocTX      31-10-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/
    --pkgctx    plog.log_ctx;
    logrow    tlogdebug%ROWTYPE;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  THAY CHO HAMG proc_reset_sequence_sync
 **  Person      Date           Comments
 **  LocTX     21/11/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_reset_txn_sequence
    IS
        p_min_value       NUMBER := 0;
        l_val             NUMBER := 0;
        l_error_desc VARCHAR2(300);
    BEGIN
        --plog.setbeginsection( pkgctx, 'pr_reset_sequence_sync' );

        l_val        := 0;
        p_min_value  := 0;

        --reset seq_core_sn_dd
        EXECUTE IMMEDIATE 'select SEQ_CORE_SN_DD.nextval from dual'
        INTO l_val;

        EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_DD increment by -' ||
                          (l_val - p_min_value) || ' minvalue ' || p_min_value;

        EXECUTE IMMEDIATE 'select SEQ_CORE_SN_DD.nextval from dual'
        INTO l_val;

        EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_DD increment by 1 minvalue ' || p_min_value;


        --reset seq_core_sn_fd
        EXECUTE IMMEDIATE 'select SEQ_CORE_SN_FD.nextval from dual'
        INTO l_val;

        EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_FD increment by -' ||
                          (l_val - p_min_value) || ' minvalue ' || p_min_value;

        EXECUTE IMMEDIATE 'select SEQ_CORE_SN_FD.nextval from dual'
        INTO l_val;

        EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_FD increment by 1 minvalue ' || p_min_value;


        --reset seq_core_sn_ln
        EXECUTE IMMEDIATE 'select SEQ_CORE_SN_LN.nextval from dual'
        INTO l_val;

        EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_LN increment by -' ||
                          (l_val - p_min_value) || ' minvalue ' || p_min_value;

        EXECUTE IMMEDIATE 'select SEQ_CORE_SN_LN.nextval from dual'
        INTO l_val;

        EXECUTE IMMEDIATE 'alter sequence SEQ_CORE_SN_LN increment by 1 minvalue ' || p_min_value;

        COMMIT;

        --plog.setendsection( pkgctx, 'pr_reset_sequence_sync' );

    EXCEPTION
    WHEN OTHERS
    THEN
        ROLLBACK;
        l_error_desc := substr(SQLERRM, 200);
        --plog.error( pkgctx, l_error_desc);

       -- plog.setendsection( pkgctx, 'pr_reset_sequence_sync' );
        RAISE;

    END;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Thay the proc_cdhist_sync
 **  Person      Date           Comments
 **  LocTX     04/11/2014    Created
 **  LocTX     05/09/2015    LocTX change from post_time to tran time

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_backup_hist(p_etl_date NUMBER)
    IS
        l_etl_date DATE;
        l_error_desc VARCHAR2(300);
        l_count NUMBER := 0;
    BEGIN
        --plog.setbeginsection (pkgctx, 'pr_backup_hist');
        l_etl_date := TO_DATE(p_etl_date, 'RRRRDDD');

        DELETE /*+ PARALLEL(16)*/ FROM sync_bk_account_history a --MOVE FROM proc_del_tran_old_sync COMMON FUNCTION
        WHERE  TRUNC(a.tran_time) < l_etl_date - 2;    --SYSDATE - 3 --#20150905 LocTX change from post time to tran time


       -- comment gather 12/10/2018  
      DBMS_STATS.gather_table_stats (
            ownname    => 'IBS',
            tabname    => 'sync_bk_account_history',
            cascade    => TRUE
       );

        SELECT (CASE
                    WHEN EXISTS(SELECT 1 
                                 FROM sync_bk_account_history a 
                                 WHERE   tran_time >= l_etl_date
                                  AND    tran_time < l_etl_date + 1
                                  AND  ROWNUM = 1
                                   -- TRUNC(a.tran_time) = l_etl_date  -- comment 09/10/2018
                                )
                    THEN 1
                    ELSE 0
                END)
        INTO l_count
        FROM DUAL;

        IF l_count = 0 THEN
            DELETE /*+ PARALLEL(16)*/ FROM sync_bk_account_history
            WHERE  sync_type IN (1, 2, 3, 5, 6, 7, 8)
            --AND    TRUNC(tran_time) = l_etl_date;--; TRUNC(SYSDATE - 1); comment 09/10/2018
            AND    tran_time >= l_etl_date
            AND    tran_time < l_etl_date + 1;


            INSERT INTO sync_bk_account_history
                    (core_sn ,
                    tran_sn ,
                    tran_service_code ,
                    trace_code ,
                    dc_sign ,
                    accepts_org ,
                    tran_type ,
                    tran_device,
                    device_no  ,
                    voucher_type ,
                    currency_code,
                    rollout_acct_no ,
                    rollout_acct_name,
                    rollout_card_no,
                    beneficiary_acct_no ,
                    beneficiary_acct_name ,
                    beneficiary_acct_bank ,
                    beneficiary_acct_branch ,
                    beneficiary_card_no ,
                    amount ,
                    fee ,
                    pre_balance,
                    act_balance ,
                    post_time,
                    tran_time ,
                    status ,
                    operator ,
                    channel ,
                    remark  ,
                    insert_date  ,
                    teller_id ,
                    tm_seq,
                    sync_type,
                    tc_code,
                    tc_sync_time,
                    lh_time,
                    due_date ,
                    os_balance )
            (SELECT core_sn ,
                    tran_sn ,
                    tran_service_code ,
                    trace_code ,
                    dc_sign ,
                    accepts_org ,
                    tran_type ,
                    tran_device ,
                    device_no ,
                    voucher_type ,
                    currency_code ,
                    rollout_acct_no ,
                    rollout_acct_name ,
                    rollout_card_no ,
                    beneficiary_acct_no ,
                    beneficiary_acct_name ,
                    beneficiary_acct_bank ,
                    beneficiary_acct_branch ,
                    beneficiary_card_no ,
                    amount,
                    fee ,
                    pre_balance ,
                    act_balance ,
                    post_time ,
                    tran_time ,
                    status ,
                    operator,
                    channel ,
                    remark ,
                    insert_date ,
                    teller_id ,
                    tm_seq ,
                    sync_type  ,
                    tc_code ,
                    tc_sync_time ,
                    ln_time  ,
                    due_date  ,
                    os_balance
            FROM   bk_account_history
            WHERE  sync_type IN (1, 2, 3, 5, 6, 7, 8)
          --AND    TRUNC(tran_time) = l_etl_date);--TRUNC(SYSDATE - 1); --1:dd transaction comment ?n index 09/10/2018
            AND   tran_time >= l_etl_date 
            AND   Tran_Time < l_etl_date + 1);


        END IF;

    -- 2019-12-18 thinhdd: This delete runs too slow, 180krows> 50mins - causing locks, change to hint
        --COMMIT; -- commit before alter to pdml
        --execute immediate 'alter session enable parallel dml';
        DELETE /*+ parallel(16) index (t(TRAN_TIME)) */ FROM bk_account_history t
        WHERE  sync_type IN (1, 2, 3, 5, 6, 7, 8)
       -- AND    TRUNC(tran_time) = l_etl_date;--TRUNC(SYSDATE - 1); comment ?n index 09/10/2018
        AND   tran_time >= l_etl_date 
        AND   Tran_Time < l_etl_date + 1 ;


        EXECUTE IMMEDIATE 'TRUNCATE TABLE twtb_account_history'; --20160301 QuanPD dung bang tam de luu giao dich

        COMMIT;
      -- comment gather 12/10/2018  
      DBMS_STATS.gather_table_stats (
            ownname    => 'IBS',
            tabname    => 'sync_bk_account_history',
            cascade    => TRUE
       ); 
      -- END comment gather 12/10/2018  
        --plog.setbeginsection (pkgctx, 'pr_backup_hist');

    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            --plog.error( pkgctx, l_error_desc);

            --plog.setendsection( pkgctx, 'pr_backup_hist' );
            RAISE;

    END;


    PROCEDURE pr_backup_hist_bk_20150905(p_etl_date NUMBER)
    IS
        l_etl_date DATE;
        l_error_desc VARCHAR2(300);
        l_count NUMBER := 0;
    BEGIN
        --plog.setbeginsection (pkgctx, 'pr_backup_hist');
        l_etl_date := TO_DATE(p_etl_date, 'RRRRDDD');

        DELETE /*+ PARALLEL(16)*/ FROM sync_bk_account_history a --MOVE FROM proc_del_tran_old_sync COMMON FUNCTION
        WHERE  TRUNC(a.post_time) < l_etl_date - 2;    --SYSDATE - 3

        SELECT (CASE
                    WHEN EXISTS(SELECT 1 FROM sync_bk_account_history a WHERE TRUNC(a.post_time) = l_etl_date )
                    THEN 1
                    ELSE 0
                END)
        INTO l_count
        FROM DUAL;

        IF l_count = 0 THEN
            DELETE /*+ PARALLEL(16)*/ FROM sync_bk_account_history
            WHERE  sync_type IN (1, 2, 3, 5, 6, 7, 8)
            AND    TRUNC(post_time) = l_etl_date;--; TRUNC(SYSDATE - 1);

            INSERT INTO sync_bk_account_history
                    (core_sn ,
                    tran_sn ,
                    tran_service_code ,
                    trace_code ,
                    dc_sign ,
                    accepts_org ,
                    tran_type ,
                    tran_device,
                    device_no  ,
                    voucher_type ,
                    currency_code,
                    rollout_acct_no ,
                    rollout_acct_name,
                    rollout_card_no,
                    beneficiary_acct_no ,
                    beneficiary_acct_name ,
                    beneficiary_acct_bank ,
                    beneficiary_acct_branch ,
                    beneficiary_card_no ,
                    amount ,
                    fee ,
                    pre_balance,
                    act_balance ,
                    post_time,
                    tran_time ,
                    status ,
                    operator ,
                    channel ,
                    remark  ,
                    insert_date  ,
                    teller_id ,
                    tm_seq,
                    sync_type,
                    tc_code,
                    tc_sync_time,
                    lh_time,
                    due_date ,
                    os_balance )
            (SELECT core_sn ,
                    tran_sn ,
                    tran_service_code ,
                    trace_code ,
                    dc_sign ,
                    accepts_org ,
                    tran_type ,
                    tran_device ,
                    device_no ,
                    voucher_type ,
                    currency_code ,
                    rollout_acct_no ,
                    rollout_acct_name ,
                    rollout_card_no ,
                    beneficiary_acct_no ,
                    beneficiary_acct_name ,
                    beneficiary_acct_bank ,
                    beneficiary_acct_branch ,
                    beneficiary_card_no ,
                    amount,
                    fee ,
                    pre_balance ,
                    act_balance ,
                    post_time ,
                    tran_time ,
                    status ,
                    operator,
                    channel ,
                    remark ,
                    insert_date ,
                    teller_id ,
                    tm_seq ,
                    sync_type  ,
                    tc_code ,
                    tc_sync_time ,
                    ln_time  ,
                    due_date  ,
                    os_balance
            FROM   bk_account_history
            WHERE  sync_type IN (1, 2, 3, 5, 6, 7, 8)
            AND    TRUNC(post_time) = l_etl_date);--TRUNC(SYSDATE - 1); --1:dd transaction


        END IF;

        DELETE /*+ PARALLEL(16)*/ FROM bk_account_history
        WHERE  sync_type IN (1, 2, 3, 5, 6, 7, 8)
        AND    TRUNC(post_time) = l_etl_date;--TRUNC(SYSDATE - 1);

        --FROM CDHIST
/*
        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        INSERT INTO sync_bk_account_history
        SELECT *
        FROM   bk_account_history a
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        DELETE FROM bk_account_history
        WHERE  (sync_type = 2 OR sync_type = 5 OR sync_type = 7) --7:cdhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1);

        --FROM DDHIST
        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        INSERT INTO sync_bk_account_history
        SELECT *
        FROM   bk_account_history a
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1);

        DELETE FROM bk_account_history
        WHERE  (sync_type = 1 OR sync_type = 5 OR sync_type = 6) --6:ddhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1)
        ;

        --LNHIST
        DELETE FROM sync_bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        INSERT INTO sync_bk_account_history
        SELECT *
        FROM   bk_account_history a
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1); --1:dd transaction

        DELETE FROM bk_account_history
        WHERE  (sync_type = 3 OR sync_type = 5 OR sync_type = 8) --8:lnhist from tmtran
        AND    TRUNC(post_time) = TRUNC(SYSDATE - 1);
*/

        COMMIT;
        --plog.setbeginsection (pkgctx, 'pr_backup_hist');

    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            --plog.error( pkgctx, l_error_desc);

            --plog.setendsection( pkgctx, 'pr_backup_hist' );
            RAISE;

    END;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Thay the proc_lnhist_cif_sync
 **  Person      Date           Comments
 **  ThanhNT     04/11/2014    Created

----------------------------------------------------------------------------------------------------*/


    PROCEDURE pr_lnhist_new_cif_sync (p_etl_date IN VARCHAR2)

    IS
        v_min_date        NUMBER;
        l_etl_date_dt        DATE;
        l_date_from_6month_dt        DATE;

        g_error_level     NUMBER;

        g_limit_time      NUMBER;
        g_min_time        NUMBER;
        g_max_time        NUMBER;
        g_time_count      NUMBER;

        v_checkpoint_date NUMBER;
        l_error_desc      VARCHAR2(300);
   BEGIN
       -- plog.setbeginsection (pkgctx, 'pr_lnhist_new_cif_sync');

        l_etl_date_dt      := TO_DATE(p_etl_date,'YYYYDDD');
        l_date_from_6month_dt := ADD_MONTHS(TRUNC(l_etl_date_dt,  'MM'), - 6);

        g_min_time        := 0;
        g_max_time        := 0;
        g_limit_time      := 0;
        g_time_count      := 0;

        v_min_date        := 0;
        v_checkpoint_date := 0;

        g_limit_time := 60000;
        g_time_count := 240000;

        g_error_level := 1;

        SELECT TO_NUMBER(TO_CHAR(l_etl_date_dt, 'yyyyddd'))
        INTO   v_checkpoint_date
        FROM   DUAL;

        SELECT TO_NUMBER(TO_CHAR(l_date_from_6month_dt, 'YYYYDDD'))
        INTO   v_min_date
        FROM   DUAL;

        INSERT INTO sync_cif_n
          SELECT cif_no,
                 status
          FROM   sync_cif
          WHERE  status = 'N'
          AND    TYPE = 'LN';

        g_error_level := 2;

        INSERT INTO sync_account_info a
        (SELECT cif_no,
              acct_no
        FROM   bk_account_info
        WHERE  cif_no IN (SELECT cif_no
                         FROM   sync_cif_n sct)
        AND    acct_type ='LN');


        g_error_level := 3;

        LOOP
            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_lnhist
              SELECT
               a.lhtran,
               a.lhpstd,
               a.lhefdt,
               a.lhdorc,
               a.lhamt,
               a.lhcur,
               a.lhcamt,
               a.lhtext,
               a.lhacct,
               a.lhseqn,
               TRIM(a.lhuser),
               a.lhtime,
               a.lhosbl,
               a.lhdudt
              FROM   --STAGINGUAT.SI_HIS_LNHIST@STAGING_PRO_CORE a,
                    STAGINGUAT.SI_HIS_LNHIST@STAGING_PRO_CORE a, --20160426 QuanPD changed STAGING->CORE
                    sync_account_info x
              WHERE  a.lhdorc IN ('D','C')
              AND    a.lhtran IN (912,993,990,914,922,915,121,101,962,974,988,42,41,976,35,15,30,62,61,23,22,21,145,143,497,496,781,889,926,102,811,812,906,43,964)--anhnt6
              AND    a.lhpstd >= v_min_date
              AND    a.lhpstd < (v_checkpoint_date)
              AND   x.acct_no = (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end)--HAONS20150120
              AND    a.lhtime >= g_min_time
              AND    a.lhtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);

        END LOOP;

        g_error_level := 4;

        DELETE /*+ PARALLEL(16)*/ FROM bk_account_history
        WHERE  rollout_acct_no IN
               (SELECT acct_no
                FROM   sync_account_info sai)
        AND    TRUNC(post_time) < l_etl_date_dt
        AND    (sync_type = 3 OR sync_type = 8)
        ;

        g_error_level := 10;

        INSERT INTO sync_tranmap
        SELECT
            a.tran_sn,
            a.teller_id,
            a.host_tran_sn,
            a.host_real_date,
            a.sender_id
        FROM   bec.bec_msglog@dblink_tranmap a
        WHERE  a.sorn = 'Y'
        AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) < l_etl_date_dt
        AND  TRUNC(TO_DATE(a.message_date,
                       'yyyymmddhh24mi')) >= l_date_from_6month_dt --#20150119 Loctx add for tunning
        AND    LENGTH(a.tran_sn) < 21;

        g_error_level := 7;

        g_min_time        := 0;
        g_max_time        := 0;

        LOOP
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                            tran_sn,
                                            device_no,
                                            ln_time,
                                            os_balance,
                                            due_date)

                (SELECT   'LN'
                          || TO_CHAR (a.lhpstd)
                          || seq_core_sn_ln.NEXTVAL,
                          TO_DATE (a.lhefdt, 'yyyyddd'),
                          TO_DATE (a.lhpstd, 'yyyyddd'),
                          TRIM (a.lhdorc),
                          a.lhamt,
                          NVL (TRIM (a.lhcur), 'VND'),
                          a.lhcamt,
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          SUBSTR (a.lhtext, 11, LENGTH (a.lhtext)),
                          (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end),
                          l_etl_date_dt,
                          '3',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.lhuser,
                          a.lhseqn,
                          c.tran_sn,
                          a.lhtran,
                          a.lhtime,
                          a.lhosbl,
                          a.lhdudt
                   FROM   sync_lnhist a, sync_tran_code d, sync_tranmap c,sync_account_info x
                  WHERE       TRIM (a.lhtran) = d.tran_service_code(+)
                          AND TRIM (a.lhuser) = c.teller_id(+)
                          AND TRIM (a.lhseqn) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.lhpstd, 'yyyyddd')) =
                                 TRUNC(TO_DATE (c.host_real_date(+),
                                                'yyyyddd'))
                          AND x.acct_no = (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end) --HAONS20150120
                                        AND a.lhtime >= g_min_time
                                        AND a.lhtime < g_max_time);
            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);

        END LOOP;

        UPDATE sync_cif
        SET    status = 'Y'
        WHERE  cif_no IN (SELECT cif_no
                          FROM   sync_cif_n)
        AND    "TYPE" = 'LN';

        COMMIT;

        EXECUTE IMMEDIATE 'alter session close database link dblink_data';

        plog.setendsection (pkgctx, 'pr_lnhist_new_cif_sync');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc || ', g_error_level =' || g_error_level);

            plog.setendsection( pkgctx, 'pr_lnhist_new_cif_sync' );
            RAISE;

   END;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Thay the proc_cdhist_cif_sync
 **  Person      Date           Comments
 **  QuanPD     04/11/2014    Created
 **  HuongDT     21/01/2015    Modify
 **  LocTx     21/01/2015    Modify

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_cdhist_new_cif_sync(p_etl_date IN VARCHAR2)
    IS
        v_min_date        NUMBER;
        l_etl_date_dt        DATE;
        l_date_from_6month_dt        DATE;

        g_error_level     NUMBER;

        g_limit_time      NUMBER;
        g_min_time        NUMBER;
        g_max_time        NUMBER;
        g_time_count      NUMBER;

        v_checkpoint_date NUMBER;
        l_error_desc      VARCHAR2(300);

    BEGIN
        plog.setbeginsection (pkgctx, 'pr_cdhist_new_cif_sync');

        l_etl_date_dt      := TO_DATE(p_etl_date,'YYYYDDD');
        l_date_from_6month_dt := ADD_MONTHS(TRUNC(l_etl_date_dt,  'MM'), - 6);

        g_min_time        := 0;
        g_max_time        := 0;
        g_limit_time      := 0;

        v_min_date        := 0;
        v_checkpoint_date := 0;

        g_limit_time := 60000;
        g_time_count := 240000;

        g_error_level := 1;

        SELECT TO_NUMBER(TO_CHAR(l_etl_date_dt, 'yyyyddd'))
        INTO   v_checkpoint_date
        FROM   DUAL;

        SELECT TO_NUMBER(TO_CHAR(l_date_from_6month_dt, 'YYYYDDD'))
        INTO   v_min_date
        FROM   DUAL;

        INSERT INTO sync_cif_n
          SELECT cif_no,
                 status
          FROM   sync_cif
          WHERE  status = 'N'
          AND    TYPE = 'CD';

        g_error_level := 2;

        INSERT INTO sync_account_info a
          (SELECT cif_no,
                  acct_no
           FROM   bk_account_info
           WHERE  cif_no IN (SELECT cif_no
                             FROM   sync_cif_n sct)
           AND    acct_type IN ('FD')
           AND    p_acct_no IS NOT NULL);

        g_error_level := 3;

        LOOP
          BEGIN
            g_min_time := g_max_time;
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO sync_cdhist
              SELECT
               a.chtran,
               a.chdorc,
               a.chamt,
               a.chcurr,
               a.chcamt,
               a.chvarf,
               a.chacct,
               a.chefdt,
               a.chpstd,
               a.chseqn,
               TRIM(a.chuser),
               a.chtime
              FROM   --STAGINGUAT.si_his_cdhist@staging_pro_core a,
                    svhispv51.cdhist@dblink_data a, --20160426 QuanPD changed STAGING->CORE
                    sync_account_info b
              WHERE  a.chdorc IN ('D','C')
              AND    a.chafft in ('B','C')
              AND    a.chtran NOT IN (77,129,178, 185)
              AND    a.chpstd >= v_min_date
              AND    a.chpstd < v_checkpoint_date
              AND TO_NUMBER(b.acct_no) = a.chacct--HAONS20150120
              AND    a.chtime >= g_min_time
              AND    a.chtime < g_max_time;

            COMMIT;
            --khong co them ban ghi nao
            EXIT WHEN(g_max_time > g_time_count);

          END;
        END LOOP;

        g_error_level := 4;

        DELETE /*+ PARALLEL(16)*/ FROM bk_account_history
        WHERE  rollout_acct_no IN
               (SELECT acct_no
                FROM   sync_account_info sai)
        AND    TRUNC(post_time) < l_etl_date_dt
        AND    (sync_type = 2 OR sync_type = 7) ;

        g_error_level := 10;

        INSERT INTO sync_tranmap
          SELECT
           a.tran_sn,
           a.teller_id,
           a.host_tran_sn,
           a.host_real_date,
           a.sender_id
          FROM   bec.bec_msglog@dblink_tranmap a
          WHERE  a.sorn = 'Y'
          AND    TRUNC(TO_DATE(a.message_date,
                               'yyyymmddhh24mi')) < l_etl_date_dt
          AND  TRUNC(TO_DATE(a.message_date,
                               'yyyymmddhh24mi')) >= l_date_from_6month_dt --#20150119 Loctx add for tunning
          AND    LENGTH(a.tran_sn) < 21;

        g_error_level := 7;

        g_min_time        := 0;
        g_max_time        := 0;

        LOOP
            BEGIN
                g_max_time := g_min_time + g_limit_time;

                INSERT INTO bk_account_history (core_sn,
                                                tran_time,
                                                post_time,
                                                dc_sign,
                                                amount,
                                                currency_code,
                                                pre_balance,
                                                channel,
                                                remark,
                                                rollout_acct_no,
                                                insert_date,
                                                sync_type,
                                                status,
                                                tran_service_code,
                                                teller_id,
                                                tm_seq,
                                                tran_sn)
                    (SELECT   'FD'
                              || TO_CHAR (a.chpstd)
                              || seq_core_sn_fd.NEXTVAL,
                              TO_DATE (a.chefdt, 'yyyyddd'),
                              TO_DATE (a.chpstd, 'yyyyddd'),
                              TRIM (a.chdorc),
                              a.chamt,
                              TRIM (a.chcurr),
                              a.chcamt,
                              DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                              SUBSTR (a.chvarf, 11, LENGTH (a.chvarf)),
                              (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end),
                              l_etl_date_dt,
                              '2',
                              'SUCC',
                              DECODE (d.tran_service_code,
                                      NULL, 'RTR001',
                                      'RTR002'),
                              a.chuser,
                              a.chseqn,
                              c.tran_sn
                       FROM   sync_cdhist a, sync_tran_code d,sync_tranmap c,sync_account_info x
                      WHERE       TRIM (a.chtran) = d.tran_service_code(+)
                              AND TRIM (a.chuser) = c.teller_id(+)
                              AND TRIM (a.chseqn) = c.host_tran_sn(+)
                              AND TRUNC (TO_DATE (a.chpstd, 'yyyyddd')) =
                                     TRUNC(TO_DATE (c.host_real_date(+),
                                                    'yyyyddd'))
                              AND x.acct_no = (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end) --HAONS20150120
                                            AND a.chtime >= g_min_time
                                            AND a.chtime < g_max_time);

                COMMIT;

                g_min_time := g_max_time;

                --khong co them ban ghi nao
                EXIT WHEN (g_max_time > g_time_count);
            END;
        END LOOP;

        UPDATE sync_cif
        SET    status = 'Y'
        WHERE  cif_no IN (SELECT cif_no
                          FROM   sync_cif_n)
        AND    "TYPE" = 'CD';

        COMMIT;

        EXECUTE IMMEDIATE 'alter session close database link dblink_data';
        plog.setendsection (pkgctx, 'pr_cdhist_new_cif_sync');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc || ', g_error_level =' || g_error_level);

            plog.setendsection( pkgctx, 'pr_cdhist_new_cif_sync' );
            RAISE;
    END;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Thay the proc_cdhist_sync
 **  Person      Date           Comments
 **  LocTX     04/11/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_cdhist_sync
    IS
        g_limit_time      NUMBER;
        g_min_time        NUMBER;
        g_max_time        NUMBER;
        g_time_count      NUMBER;
        l_error_desc      VARCHAR2(300);
        l_check NUMBER;
    BEGIN

        plog.setbeginsection (pkgctx, 'pr_cdhist_sync');
        g_min_time   := 0;
        g_max_time   := 0;
        g_limit_time := 60000;
        g_time_count := 240000;

        -- comment  10/16/2018
        DBMS_STATS.gather_table_stats (
                ownname    => 'IBS',
                tabname    => 'sync_etl_cdhist'
                 );

        DBMS_STATS.gather_table_stats (
                ownname    => 'IBS',
                tabname    => 'sync_etl_tranmap'
                 );
        --
        --#20150415 Loctx add (giu nguyen source code goc)
        INSERT INTO sync_account_info a
        (SELECT bai.cif_no,
              bai.acct_no
        FROM   bk_account_info bai
        WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
        AND    bai.acct_type IN ('FD')
        AND    bai.p_acct_no IS NOT NULL);


        LOOP
        BEGIN
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO twtb_account_history (core_sn, --20160301 QuanPD dung bang tam roi moi day vao bang chinh
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                            tran_sn)
                (SELECT 'FD'|| TO_CHAR (a.chpstd)
                          || seq_core_sn_fd.NEXTVAL,
                          TO_DATE (a.chefdt, 'yyyyddd'),
                          TO_DATE (a.chpstd, 'yyyyddd'),
                          TRIM (a.chdorc),
                          a.chamt,
                          TRIM (a.chcurr),
                          a.chcamt,
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          SUBSTR (a.chvarf, 11, LENGTH (a.chvarf)),
                          (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end),
                          SYSDATE,
                          '7',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.chuser,
                          a.chseqn,
                          c.tran_sn
                   FROM   sync_etl_cdhist a,
                            sync_tran_code d,
                            sync_etl_tranmap c,
                            sync_account_info info
                  WHERE       TRIM (a.chtran) = d.tran_service_code(+)
                        AND TRIM (a.chuser) = c.teller_id(+)
                        AND a.chseqn = c.host_tran_sn(+)
                        AND TO_CHAR(a.chpstd) = c.host_real_date(+)
                        AND info.acct_no = (case when length(a.chacct) = 13 then LPAD (a.chacct, 14, '0') else TO_CHAR(a.chacct) end) 
                        AND a.chtime >= g_min_time
                        AND a.chtime < g_max_time);


                COMMIT;

                g_min_time := g_max_time;

                --khong co them ban ghi nao
                EXIT WHEN (g_max_time > g_time_count);
            END;
        END LOOP;
        COMMIT;
        plog.setbeginsection (pkgctx, 'pr_cdhist_sync');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);

            plog.setendsection( pkgctx, 'pr_cdhist_sync' );
            RAISE;

    END;


/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Thay the proc_ddhist_sync
 **  Person      Date           Comments
 **  LocTX     04/11/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_ddhist_sync
    IS
        g_limit_time      NUMBER;
        g_min_time        NUMBER;
        g_max_time        NUMBER;
        g_time_count      NUMBER;
        l_error_desc      VARCHAR2(300);
        l_check           NUMBER;
    BEGIN

        plog.setbeginsection (pkgctx, 'pr_ddhist_sync');
        g_limit_time := 60000;
        g_time_count := 240000;
        g_min_time        := 0;
        g_max_time        := 0;
        -- comment 16/10/2018
        DBMS_STATS.gather_table_stats (
                ownname    => 'IBS',
                tabname    => 'sync_etl_ddhist'
            ); 
        --#20150415 Loctx add (giu nguyen source code goc)
        INSERT INTO sync_account_info a
          (SELECT bai.cif_no,
                  bai.acct_no
           FROM   bk_account_info bai
           WHERE  bai.cif_no IN (
                                 SELECT cif_no
                                 FROM   bb_corp_info)
           AND    bai.acct_type IN ('CA',
                                    'SA', '31'));

    --noformat start
        LOOP
        BEGIN

            g_max_time := g_min_time + g_limit_time;

            INSERT INTO twtb_account_history (core_sn, --20160301 QuanPD dung bang tam roi moi day vao bang chinh
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                            tran_sn,
                                            trace_code,
                                            device_no --#20151029 LocTX add
                                             )
                (SELECT
                             'DD'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_dd.NEXTVAL,
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          TO_DATE (
                              --(a.treffd || ':' || LPAD (a.trtime, 6, '0')),--#20150324 LocTX change
                              (DECODE(a.treffd, 0, a.trdate, a.treffd) || ':' || LPAD (a.trtime, 6, '0')),--#20150324 LocTX change
                              'yyyyddd:hh24miss'),
                          TRIM (a.dorc),
                          NVL(a.amt,0),
                          TRIM (a.trctyp),
                          a.camt,
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          SYSDATE,
                          6,
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.truser,
                          a.seq,
                          c.tran_sn,
                          TRIM(a.auxtrc),
                          a.trancd --#20151029 LocTX add
                   FROM   sync_etl_ddhist a,
                           sync_tran_code d,
                           sync_etl_tranmap c,
                           sync_account_info info
                  WHERE       TRIM (a.auxtrc) = d.tran_service_code(+)
                        AND TRIM (a.truser) = c.teller_id(+)
                        AND a.seq = c.host_tran_sn(+)
                        AND a.trdate = c.host_real_date(+) --#20150415 LocTX change
                        AND info.acct_no = (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end) 
                        AND a.trtime >= g_min_time
                        AND a.trtime < g_max_time);

            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        END;
        END LOOP;
        COMMIT;
        plog.setbeginsection (pkgctx, 'pr_ddhist_sync');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM,200) || g_min_time; --#20150324 Loctx add g_min_time
            plog.error( pkgctx, l_error_desc);

            plog.setendsection( pkgctx, 'pr_ddhist_sync' );
            RAISE;
    END;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Thay the proc_ddhist_sync
 **  Person      Date           Comments
 **  LocTX     04/11/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_lnhist_sync IS
        g_limit_time      NUMBER;
        g_min_time        NUMBER;
        g_max_time        NUMBER;
        g_time_count      NUMBER;
        l_error_desc      VARCHAR2(300);
        l_check           NUMBER;
    BEGIN
        plog.setbeginsection (pkgctx, 'pr_lnhist_sync');
        g_limit_time := 60000;
        g_time_count := 240000;
        g_min_time        := 0;
        g_max_time        := 0;
        -- comment 16/10/2018
        DBMS_STATS.gather_table_stats (
                ownname    => 'IBS',
                tabname    => 'sync_etl_lnhist'
                 );
        --#20150415 Loctx add (giu nguyen source code goc)

        INSERT INTO sync_account_info a
        (SELECT bai.cif_no,
              bai.acct_no
        FROM   bk_account_info bai
        WHERE  bai.cif_no IN (
                             SELECT cif_no
                             FROM   bb_corp_info)
        AND    bai.acct_type IN ('LN'));

        LOOP
            g_max_time := g_min_time + g_limit_time;

             plog.DEBUG (pkgctx, 'g_max_time=' || g_max_time);

            INSERT INTO twtb_account_history (core_sn, --20160301 QuanPD dung bang tam roi moi day vao bang chinh
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                            tran_sn,
                                            device_no,
                                            ln_time,
                                            os_balance,
                                            due_date)
                                        (SELECT 'LN'|| TO_CHAR (a.lhpstd)
                                        || seq_core_sn_ln.NEXTVAL,
                                        TO_DATE (a.lhefdt, 'yyyyddd'),
                                        TO_DATE (a.lhpstd, 'yyyyddd'),
                                        TRIM (a.lhdorc),
                                        a.lhamt,
                                        NVL (TRIM (a.lhcur), 'VND'),
                                        a.lhcamt,
                                        DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                                        SUBSTR (a.lhtext, 11, LENGTH (a.lhtext)),
                                        (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end),
                                        SYSDATE,
                                        '8',
                                        'SUCC',
                                        DECODE (d.tran_service_code,
                                          NULL, 'RTR001',
                                          'RTR002'),
                                        a.lhuser,
                                        a.lhseqn,
                                        c.tran_sn,
                                        a.lhtran,
                                        a.lhtime,
                                        a.lhosbl,
                                        a.lhdudt
                    FROM    sync_etl_lnhist a,
                            sync_tran_code d,
                            sync_etl_tranmap c,
                            sync_account_info info
                        WHERE TRIM (a.lhtran) = d.tran_service_code(+)
                            AND TRIM (a.lhuser) = c.teller_id(+)
                            AND a.lhseqn = c.host_tran_sn(+)
                            AND TO_CHAR(a.lhpstd) = c.host_real_date(+)
                            AND info.acct_no = (case when length(a.lhacct) = 13 then LPAD (a.lhacct, 14, '0') else TO_CHAR(a.lhacct) end)
                            AND a.lhtime >= g_min_time
                            AND a.lhtime < g_max_time);

            plog.DEBUG (pkgctx, 'finish g_max_time=' || g_max_time);
            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        END LOOP;
        COMMIT;
        plog.setbeginsection (pkgctx, 'pr_lnhist_sync');
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);

            plog.setendsection( pkgctx, 'pr_lnhist_sync' );
        RAISE;

    END;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Thay the proc_ddhist_cif_sync
 **  Person      Date           Comments
 **  HuongDT     04/11/2014    Created
 **  HuongDT     21/01/2015    Modify
 **  LocTx     21/01/2015    Modify

----------------------------------------------------------------------------------------------------*/

PROCEDURE pr_ddhist_new_cif_sync(p_etl_date  IN VARCHAR2)
  IS
    v_min_date        NUMBER;
    l_etl_date_dt        DATE;
    l_date_from_6month_dt        DATE;

    g_error_level     NUMBER;

    g_limit_time      NUMBER;
    g_min_time        NUMBER;
    g_max_time        NUMBER;
    g_time_count      NUMBER;

    v_checkpoint_date NUMBER;
    l_error_desc      VARCHAR2(300);

  BEGIN
    plog.setbeginsection( pkgctx, 'pr_ddhist_new_cif_sync' );
    l_etl_date_dt      := TO_DATE(p_etl_date,'YYYYDDD');
    l_date_from_6month_dt := ADD_MONTHS(TRUNC(l_etl_date_dt,  'MM'), - 6);


    g_min_time        := 0;
    g_max_time        := 0;
    g_limit_time      := 0;
    g_time_count      := 0;


    v_min_date        := 0;
    v_checkpoint_date := 0;

    g_limit_time := 60000;
    g_time_count := 240000;

    g_error_level := 1;

    SELECT TO_NUMBER(TO_CHAR(l_etl_date_dt, 'yyyyddd'))
    INTO   v_checkpoint_date
    FROM   DUAL;

    SELECT TO_NUMBER(TO_CHAR(l_date_from_6month_dt, 'YYYYDDD'))
    INTO   v_min_date
    FROM   DUAL;

    INSERT INTO sync_cif_n
      SELECT cif_no,
             status
      FROM   sync_cif
      WHERE  status = 'N'
      AND    TYPE = 'DD';

    g_error_level := 2;

    INSERT INTO sync_account_info a
      (SELECT bai.cif_no,
              bai.acct_no
       FROM   bk_account_info bai
       WHERE  bai.cif_no IN (SELECT cif_no
                             FROM   sync_cif_n sct)
       AND    bai.acct_type IN ('CA','SA'));

    --Get  data from DDHIST core
    g_error_level := 3;

    LOOP
      BEGIN
        g_min_time := g_max_time;
        g_max_time := g_min_time + g_limit_time;

        INSERT INTO sync_ddhist
          SELECT 
           a.trancd,
           a.trdat6,
           a.dorc,
           a.amt,
           a.trctyp,
           a.camt,
           a.trefth,
           a.tracct,
           a.trdate,
           a.treffd,
           a.trtime,
           a.seq,
           TRIM(a.truser),
           TRIM(a.auxtrc)
          FROM   --STAGINGUAT.si_his_ddhist@STAGING_PRO_core a,
                STAGINGUAT.SI_HIS_DDHIST@STAGING_PRO_CORE a, --20160426 QuanPD changed STAGING->CORE
                sync_account_info x
          WHERE  a.dorc IN ('D','C')
          AND    a.trancd NOT IN (77,129,178,185)
          AND    a.trdate >= v_min_date
          AND    a.trdate < (v_checkpoint_date)
          AND  x.acct_no = (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end)--HAONS20150120
          AND    a.trtime >= g_min_time
          AND    a.trtime < g_max_time;

           COMMIT;
        --khong co them ban ghi nao
        EXIT WHEN(g_max_time > g_time_count);
      END;
    END LOOP;

    g_error_level := 4;

    --EXECUTE IMMEDIATE 'alter table bk_account_history parallel 8';
    --EXECUTE IMMEDIATE 'alter session force parallel dml';

    DELETE /*+ PARALLEL(bk_account_history,8) */ FROM bk_account_history
    WHERE  rollout_acct_no IN
           (SELECT acct_no
            FROM   sync_account_info sai)
    AND    TRUNC(post_time) < l_etl_date_dt
    AND    (sync_type = 1 OR sync_type = 5 OR sync_type = 6);

    --EXECUTE IMMEDIATE 'alter table bk_account_history noparallel';

    g_error_level := 10;

    INSERT INTO sync_tranmap
      SELECT
       a.tran_sn,
       a.teller_id,
       a.host_tran_sn,
       a.host_real_date,
       a.sender_id
      FROM   bec.bec_msglog@dblink_tranmap a
      WHERE  a.sorn = 'Y'
      AND    TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) < l_etl_date_dt
      AND  TRUNC(TO_DATE(a.message_date,
                           'yyyymmddhh24mi')) >= l_date_from_6month_dt --#20150119 Loctx add for tunning

      AND    LENGTH(a.tran_sn) < 21;

    g_error_level := 7;

    g_min_time        := 0;
    g_max_time        := 0;

    --noformat start
    LOOP
        BEGIN
            g_max_time := g_min_time + g_limit_time;

            INSERT INTO bk_account_history (core_sn,
                                            tran_time,
                                            post_time,
                                            dc_sign,
                                            amount,
                                            currency_code,
                                            pre_balance,
                                            channel,
                                            remark,
                                            rollout_acct_no,
                                            insert_date,
                                            sync_type,
                                            status,
                                            tran_service_code,
                                            teller_id,
                                            tm_seq,
                                            tran_sn,
                                            trace_code
                                                                            )
                (SELECT   'DD'
                          || TO_CHAR (a.trdate)
                          || seq_core_sn_dd.NEXTVAL,
                          TO_DATE (
                              (a.trdate || ':' || LPAD (a.trtime, 6, '0')),
                              'yyyyddd:hh24miss'),
                          (CASE
                                WHEN a.treffd IS NULL OR a.treffd = 0 THEN NULL --#20150508 LocTX add case when
                                ELSE
                                  TO_DATE (
                                      (a.treffd || ':' || LPAD (a.trtime, 6, '0')),
                                      'yyyyddd:hh24miss')
                                END
                          ),
                          TRIM (a.dorc),
                          a.amt,
                          TRIM (a.trctyp),
                          a.camt,
                          DECODE (c.sender_id, NULL, 'CNT', c.sender_id),
                          SUBSTR (a.trefth, 11, LENGTH (a.trefth)),
                          (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end),
                          l_etl_date_dt,
                          '1',
                          'SUCC',
                          DECODE (d.tran_service_code,
                                  NULL, 'RTR001',
                                  'RTR002'),
                          a.truser,
                          a.seq,
                                        c.tran_sn,
                                        TRIM(a.auxtrc)
                   FROM   sync_ddhist a, sync_tran_code d, sync_tranmap c,sync_account_info x
                  WHERE       TRIM (a.auxtrc) = d.tran_service_code(+)
                          AND TRIM (a.truser) = c.teller_id(+)
                          AND TRIM (a.seq) = c.host_tran_sn(+)
                          AND TRUNC (TO_DATE (a.trdate, 'yyyyddd')) = TRUNC(TO_DATE (c.host_real_date(+), 'yyyyddd'))
                          AND x.acct_no = (case when length(a.tracct) = 13 then LPAD (a.tracct, 14, '0') else TO_CHAR(a.tracct) end)--HAONS20150120
                            AND a.trtime >= g_min_time
                            AND a.trtime < g_max_time

                    /*
                   FROM   sync_ddhist a
                   LEFT JOIN sync_tran_code d ON TRIM (a.auxtrc) = d.tran_service_code
                   LEFT JOIN sync_tranmap c ON TRIM (a.truser) = c.teller_id AND a.seq = c.host_tran_sn--,sync_account_info x
                                   AND TRUNC (TO_DATE (a.trdate, 'yyyyddd')) = TRUNC(TO_DATE (c.host_real_date, 'yyyyddd'))
                  WHERE 1 = 1
                          AND LPAD (a.tracct, 14, '0') IN (SELECT acct_no FROM sync_account_info)
                                        AND a.trtime >= g_min_time
                                        AND a.trtime < g_max_time
                    */
                                        );
            COMMIT;

            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        END;
    END LOOP;


    UPDATE sync_cif
    SET    status = 'Y'
    WHERE  cif_no IN (SELECT cif_no
                      FROM   sync_cif_n)
    AND    "TYPE" = 'DD';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';
    plog.setendsection (pkgctx, 'pr_ddhist_new_cif_sync');

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc || ', g_error_level =' || g_error_level);

            plog.setendsection( pkgctx, 'pr_ddhist_new_cif_sync' );
            RAISE;
    END;

/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  Cap nhat lai thong tin nguoi thu huong
 **  Person      Date           Comments
 **  LocTX     08/12/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_update_benifit_info( p_etl_date IN NUMBER)
    IS
        g_limit_time      NUMBER;
        g_min_time        NUMBER;
        g_max_time        NUMBER;
        g_time_count      NUMBER;
        l_error_desc      VARCHAR2(300);
        l_etl_date_dt   DATE;
    BEGIN
        plog.setbeginsection (pkgctx, 'pr_update_benifit_info');

        l_etl_date_dt := TO_DATE(p_etl_date , 'RRRRDDD');

        g_min_time   := 0;
        g_max_time   := 0;
        g_limit_time := 60000;
        g_time_count := 240000;

          -- gather static 11/10/2018
        DBMS_STATS.gather_table_stats (
            ownname    => 'IBS',
            tabname    => 'twtb_account_history_benifit',
            cascade    => TRUE
              );

       DBMS_STATS.gather_table_stats (
            ownname    => 'IBS',
            tabname    => 'sync_cdc_tmtran',
            cascade    => TRUE
       );

       -- gather static 11/10/2018

        LOOP
        BEGIN
            g_max_time := g_min_time + g_limit_time;
            UPDATE twtb_account_history c SET (
                    c.beneficiary_acct_no,
                    c.beneficiary_acct_bank,
                    c.beneficiary_acct_name
                ) =
                (SELECT MAX(a.beneficiary_acct_no), -- #20160512 Loctx change from DISTINCT to MAX
                        MAX(a.beneficiary_acct_bank),
                        MAX(a.beneficiary_acct_name)
                FROM sync_bk_account_history a
                WHERE TRUNC(a.post_time) = l_etl_date_dt
                AND TO_NUMBER(TO_CHAR( a.post_time, 'hh24miss')) >= g_min_time
                AND TO_NUMBER(TO_CHAR( a.post_time, 'hh24miss')) < g_max_time
                AND c.trace_code = a.trace_code
                AND c.device_no = a.device_no     --#20150423 Loctx bo sung
                AND c.rollout_acct_no = a.rollout_acct_no
                AND c.dc_sign = a.dc_sign
                AND c.tm_seq = a.tm_seq
                AND c.teller_id = a.teller_id
                AND c.post_time = a.post_time
                AND c.amount = a.amount
                AND c.currency_code = a.currency_code
                AND a.beneficiary_acct_no is not null --20150324 HAONS add
                )
            WHERE TRUNC(c.post_time) = l_etl_date_dt
                AND TO_NUMBER(TO_CHAR( c.post_time, 'hh24miss')) >= g_min_time
                AND TO_NUMBER(TO_CHAR( c.post_time, 'hh24miss')) < g_max_time
            ;

            COMMIT;

            DBMS_OUTPUT.put_line('b1');
            g_min_time := g_max_time;

            --khong co them ban ghi nao
            EXIT WHEN (g_max_time > g_time_count);
        END;
        END LOOP;
        DBMS_OUTPUT.put_line('b2');
        --#20150422 LocTX add for giao dich phat sinh luc chay batch
        plog.info( pkgctx, 'Cap nhat thong tin thu huong - giao dich phat sinh do chay batch');
        DBMS_OUTPUT.put_line('b3');

        --pr_benifit_batch_txn(p_etl_date);--#20171225 Loctx disable va thay bang ham khac xac dinh thong tin thu huong
        cspkg_benifit_refresh.pr_benifit_internal_txn_eod(p_etl_date);--#20171225 Loctx add
        DBMS_OUTPUT.put_line('b4');

        plog.setendsection (pkgctx, 'pr_update_benifit_info');
        DBMS_OUTPUT.put_line('b5');

        -- gather comment 16/10/2018
         DBMS_STATS.gather_table_stats (
            ownname    => 'IBS',
            tabname    => 'twtb_account_history',
            cascade    => TRUE
       );
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200) || 'g_max_time=' || g_max_time || ',g_min_time' || g_min_time;
            plog.error( pkgctx, l_error_desc);

            plog.setendsection( pkgctx, 'pr_update_benifit_info' );
        RAISE;

    END;


--#20141205 Loctx move from orgnial source code
-- tamo thoi disble vi chua tao duoc DB link
  -- chay 1 lan trong ngay, lich su truoc hien tai 1 ngay. CAP NHAT THONG TIN NGUOI THU HUONG
  PROCEDURE proc_ddft_transaction_by_date (p_etl_date NUMBER)
  IS
  BEGIN
    NULL;
  /*
      MERGE INTO bk_account_history c
       USING (SELECT DISTINCT a.trn_date,
                              a.channel_trn,
                              a.seq_number,
                              a.user_id,
                              a.trf_bank_sk,
                              a.related_person,
                              LPAD (b.account_no, 14, '0') account_no,
                              a.related_account
                FROM     dwh.ddft_transaction@dblink_balance a
                     INNER JOIN
                         dwh.dddm_account@dblink_balance b
                     ON a.account_no_sk = b.account_no_sk
               WHERE     a.channel_trn IS NOT NULL
                     AND trn_date = p_etl_date
                     AND a.dorc_ind IS NOT NULL
                     AND (   related_person IS NOT NULL
                          OR related_account IS NOT NULL
                          OR trf_bank_sk IS NOT NULL)
                     AND a.trn_sk != 70351) src
          ON (    c.trace_code IS NOT NULL
              AND c.rollout_acct_no = src.account_no
              AND TO_CHAR (c.tran_time, 'yyyyddd') = src.trn_date
              AND c.trace_code = src.channel_trn
              AND c.tm_seq = src.seq_number
              AND TRIM (c.teller_id) = src.user_id)
      WHEN MATCHED
      THEN
          UPDATE SET
              c.beneficiary_acct_no = TRIM (src.related_account),
              c.beneficiary_acct_bank = TRIM (src.trf_bank_sk),
              c.beneficiary_acct_name = TRIM (src.related_person);

      COMMIT;

      MERGE INTO bk_account_history c
       USING (SELECT DISTINCT a.trn_date,
                              a.channel_trn,
                              a.seq_number,
                              a.user_id,
                              a.trf_bank_sk,
                              a.related_person,
                              LPAD (b.account_no, 14, '0') account_no,
                              a.related_account
                FROM     dwh.ddft_transaction@dblink_balance a
                     INNER JOIN
                         dwh.dddm_account@dblink_balance b
                     ON a.account_no_sk = b.account_no_sk
               WHERE     a.channel_trn IS NOT NULL
                     AND trn_date = p_etl_date
                     AND a.dorc_ind IS NOT NULL
                     AND (   related_person IS NOT NULL
                          OR related_account IS NOT NULL
                          OR trf_bank_sk IS NOT NULL)
                     AND a.trn_sk = 70351) src
          ON (    c.trace_code IS NOT NULL
              AND c.rollout_acct_no = src.account_no
              AND TO_CHAR (c.tran_time, 'yyyyddd') = src.trn_date
              AND c.trace_code = src.channel_trn
              AND c.tm_seq = src.seq_number
              AND TRIM (c.teller_id) = src.user_id)
      WHEN MATCHED
      THEN
          UPDATE SET
              c.beneficiary_acct_no = TRIM (src.related_account),
              c.beneficiary_acct_bank = TRIM (src.trf_bank_sk),
              c.beneficiary_acct_name = TRIM (src.related_person);

      COMMIT;
*/
  END;

  --#20160301 QuanPD Added
    -- Cap nhat thong tin nguoi thu huong cho nhung giao dich phat sinh do chay batch
  -- Update thong tin vao bang tam
  PROCEDURE pr_benifit_batch_txn(p_date7 NUMBER)
    IS

        CURSOR c_txn (p_cur_date7 NUMBER, p_cur_date_dt DATE)
        IS
                SELECT h1.rowid h_rowid, h2.rollout_acct_no beneficiary_acct_no,
                        'MARITIME BANK' beneficiary_acct_bank,
                        bka.acct_name beneficiary_acct_name
                FROM twtb_account_history h1,
                    twtb_account_history h2,
                    cstb_account_info bka
                WHERE 1=1
                AND TRUNC(h1.post_time) = p_cur_date_dt
                AND TRUNC(h2.post_time) = p_cur_date_dt
                AND h1.trace_code = h2.trace_code--channel txn - ma giao dich
                AND h1.teller_id = h2.teller_id -- ma giao dich vien
                AND h1.dc_sign <> h2.dc_sign -- D and C
                AND bka.acct_no(+) = h2.rollout_acct_no
                AND h2.trace_code NOT IN
                                    ('8121',
                                        '8122',
                                        '8123',
                                        '8124',
                                        '8125',
                                        '8126',
                                        '8161',
                                        '8162',
                                        '8163',
                                        '8171',
                                        '8172',
                                        '8173',
                                        '8176',
                                        '8177',
                                        '8178')
                AND NVL(h1.beneficiary_acct_bank, '$NULL$') = '$NULL$';


        TYPE ty_txn_tb IS TABLE OF c_txn%ROWTYPE;

        l_txn_list ty_txn_tb;

        l_error_desc VARCHAR2(300);

    BEGIN

        plog.setbeginsection( pkgctx, 'pr_benifit_batch_txn' );

        OPEN c_txn(p_date7, TO_DATE(p_date7, 'RRRRDDD'));
        LOOP
            FETCH c_txn
            BULK COLLECT INTO l_txn_list
            LIMIT  10000;

            FORALL i IN 1..l_txn_list.count
                UPDATE twtb_account_history SET beneficiary_acct_no = l_txn_list(i).beneficiary_acct_no,
                                                beneficiary_acct_name = l_txn_list(i).beneficiary_acct_name,
                                                beneficiary_acct_bank = l_txn_list(i).beneficiary_acct_bank
                WHERE rowid = l_txn_list(i).h_rowid;
            COMMIT;

            EXIT WHEN c_txn%NOTFOUND;

        END LOOP;
        CLOSE c_txn ;
        plog.setendsection( pkgctx, 'pr_benifit_batch_txn' );
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            plog.error( pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_benifit_batch_txn' );
            CLOSE c_txn;
            --forward error
            RAISE;
    END;

  BEGIN
    SELECT   *
    INTO   logrow
    FROM   tlogdebug
    WHERE   ROWNUM <= 1;

    pkgctx      :=
    plog.init('cspkg_transaction_etl_sync_2',
        plevel => logrow.loglevel,
        plogtable => ( logrow.log4table = 'Y' ),
        palert => ( logrow.log4alert = 'Y' ),
        ptrace => ( logrow.log4trace = 'Y' ) );

  END cspkg_transaction_etl_sync_2;

/
