--------------------------------------------------------
--  DDL for Package Body EBANK_CIF_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "EBANK_CIF_SYNC" IS

  v_start_date DATE;

  g_error_level NUMBER;

  g_loop_count NUMBER;

  g_limit_count NUMBER;

  g_min_count NUMBER;

  g_max_count NUMBER;

  g_cif_count NUMBER;

  g_error_count NUMBER;

  g_error_sub_count NUMBER;

  g_sub_loop_count NUMBER;

  PROCEDURE proc_mobile_sync IS
    v_checkpoint_date NUMBER;
    v_checkpoint_time NUMBER;
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count      := 10;
    g_limit_count     := 200000;
    g_min_count       := 0;
    g_max_count       := 0;
    g_cif_count       := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    g_sub_loop_count  := 0;
    v_checkpoint_date := 0;
    v_checkpoint_time := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    SELECT to_number(to_char(SYSDATE,
                             'yyyy') || to_char(SYSDATE,
                                                'mm') ||
                     to_char(SYSDATE,
                             'dd'))
    INTO   v_checkpoint_date
    FROM   dual;

    SELECT a.sync_count
    INTO   v_checkpoint_time
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CIFADD';

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
            INSERT INTO sync_cfconn
              SELECT /*+ ALL_ROWS */
               b.cfaccn,
               b.cfeadd,
               b.cfeadc,
               b.cfzseq,
               b.cfatyp,
               b.cfcifn,
               b.cfatim
              FROM   svdatpv51.cfconn@DBLINK_DATA b
              WHERE  b.cfadlm <> 0
              AND    b.cfatim >= v_checkpoint_time
              AND    b.cfadlm >= v_checkpoint_date
              AND    b.cfeadc = 'MP'
              AND    b.cfatyp = 'C'
              AND    length(rtrim(b.cfeadd)) < 21
              AND    b.cfaccn BETWEEN g_min_count AND g_max_count;

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


    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    SELECT MAX(a.cfaccn)
    INTO   g_cif_count
    FROM   sync_cfconn a;
    SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO bk_cif c
            USING (SELECT TRIM(sc.cfaccn) cfaccn,
                          TRIM(sc.cfeadd) addr
                   FROM   sync_cfconn sc
                   --WHERE  sc.cfeadc = 'MP' --AND    sc.cfatyp = 'C'
                   WHERE  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
                                       FROM   sync_cfconn sc2
                                       WHERE  sc2.cfaccn = sc.cfaccn
                                       AND    sc2.cfatyp = sc.cfatyp
                                       AND    sc2.cfeadc = sc.cfeadc)
                  AND  sc.cfaccn BETWEEN g_min_count AND g_max_count
                  --AND    sc.cfaccn = cif_tab(i)
                  ) a
            ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
            WHEN MATCHED THEN
              UPDATE
              SET    c.mobile = a.addr;

              COMMIT;

              g_min_count := g_max_count;

              --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);

           EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_merge_bulk;
              g_error_count := g_error_count + 1;
              IF (g_error_count >= 10)
              THEN
                RAISE;
              END IF;
        END;
        END LOOP;
        --noformat end

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;
    SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk_1;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
              --Update 2011/07/30, also sync to bc_user_info
--              MERGE INTO bc_user_info c
--              USING (SELECT TRIM(sc.cfaccn) cfaccn,
--                            TRIM(sc.cfeadd) addr
--                     FROM   sync_cfconn sc
--                     --WHERE  sc.cfeadc = 'MP'
--                     --AND    sc.cfatyp = 'C'
--                     WHERE  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
--                                         FROM   sync_cfconn sc2
--                                         WHERE  sc2.cfaccn = sc.cfaccn
--                                         AND    sc2.cfatyp = sc.cfatyp
--                                         AND    sc2.cfeadc = sc.cfeadc)
--                    AND sc.cfaccn BETWEEN g_min_count AND g_max_count
--                    --AND    sc.cfaccn = cif_tab(i)
--                    ) a
--              ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
--              WHEN MATCHED THEN
--                UPDATE
--                SET    c.mobile = a.addr;
--    
--                COMMIT;

              g_min_count := g_max_count;

              --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);

           EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_merge_bulk_1;
              g_error_count := g_error_count + 1;
              IF (g_error_count >= 10)
              THEN
                RAISE;
              END IF;
        END;
        END LOOP;
        --noformat end

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_mobile_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_mobile_sync',
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
  PROCEDURE proc_email_sync IS
    v_checkpoint_date NUMBER;
    v_checkpoint_time NUMBER;
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count      := 10;
    g_limit_count     := 200000;
    g_min_count       := 0;
    g_max_count       := 0;
    g_cif_count       := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    v_checkpoint_date := 0;
    v_checkpoint_time := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    SELECT to_number(to_char(SYSDATE,
                             'yyyy') || to_char(SYSDATE,
                                                'mm') ||
                     to_char(SYSDATE,
                             'dd'))
    INTO   v_checkpoint_date
    FROM   dual;

    SELECT a.sync_count
    INTO   v_checkpoint_time
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CIFADD';

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

            INSERT INTO sync_cfconn
              SELECT /*+ ALL_ROWS */
               b.cfaccn,
               b.cfeadd,
               b.cfeadc,
               b.cfzseq,
               b.cfatyp,
               b.cfcifn,
               b.cfatim
              FROM   svdatpv51.cfconn@DBLINK_DATA b
              WHERE  b.cfadlm <> 0
              AND    b.cfatim >= v_checkpoint_time
              AND    b.cfadlm >= v_checkpoint_date
              AND    b.cfeadc = 'EM'
              AND    b.cfatyp = 'C'
                    --AND    length(rtrim(b.cfeadd)) < 21
              AND    b.cfaccn BETWEEN g_min_count AND g_max_count;

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


    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    SELECT MAX(a.cfaccn)
    INTO   g_cif_count
    FROM   sync_cfconn a;
    SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO bk_cif c
            USING (SELECT TRIM(sc.cfaccn) cfaccn,
                          TRIM(sc.cfeadd) addr
                   FROM   sync_cfconn sc
                   /*WHERE  sc.cfeadc = 'EM'
                   AND    sc.cfatyp = 'C'*/
                   WHERE  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
                                       FROM   sync_cfconn sc2
                                       WHERE  sc2.cfaccn = sc.cfaccn
                                       AND    sc2.cfatyp = sc.cfatyp
                                       AND    sc2.cfeadc = sc.cfeadc)
                   AND sc.cfaccn BETWEEN g_min_count AND g_max_count
                   --AND    sc.cfaccn = cif_tab(i)
                  ) a
            ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
            WHEN MATCHED THEN
              UPDATE
              SET    c.email = a.addr;

          COMMIT;

          g_min_count := g_max_count;

                --khong co them ban ghi nao
              EXIT WHEN(g_max_count > g_cif_count);

             EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;
                IF (g_error_count >= 10)
                THEN
                  RAISE;
                END IF;
          END;
        END LOOP;
        --noformat end

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;
    SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            --Update 2011/07/30, also update to bc_user_info
--            MERGE INTO bc_user_info c
--            USING (SELECT TRIM(sc.cfaccn) cfaccn,
--                          TRIM(sc.cfeadd) addr
--                   FROM   sync_cfconn sc
--                   /*WHERE  sc.cfeadc = 'EM'
--                   AND    sc.cfatyp = 'C'*/
--                   WHERE  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
--                                       FROM   sync_cfconn sc2
--                                       WHERE  sc2.cfaccn = sc.cfaccn
--                                       AND    sc2.cfatyp = sc.cfatyp
--                                       AND    sc2.cfeadc = sc.cfeadc)
--                   AND sc.cfaccn BETWEEN g_min_count AND g_max_count
--                   --AND    sc.cfaccn = cif_tab(i)
--                  ) a
--            ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
--            WHEN MATCHED THEN
--              UPDATE
--              SET    c.email = a.addr;
--          COMMIT;

          g_min_count := g_max_count;

                --khong co them ban ghi nao
              EXIT WHEN(g_max_count > g_cif_count);

             EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;
                IF (g_error_count >= 10)
                THEN
                  RAISE;
                END IF;
          END;
        END LOOP;
        --noformat end

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_email_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_email_sync',
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
  PROCEDURE proc_telephone_sync IS
  BEGIN
    v_start_date      := SYSDATE;
    g_loop_count      := 10;
    g_limit_count     := 200000;
    g_min_count       := 0;
    g_max_count       := 0;
    g_cif_count       := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

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

            INSERT INTO sync_cfconn
              SELECT /*+ ALL_ROWS */
               TRIM(b.cfaccn),
               b.cfeadd,
               TRIM(b.cfeadc),
               b.cfzseq,
               TRIM(b.cfatyp),
               CFCIFN,
               b.cfatim
              FROM   svdatpv51.cfconn@DBLINK_DATA b
              WHERE  b.cfadlm <> 0
              AND    TRUNC(to_date(b.cfadlm,
                                   'yyyyddd')) >= TRUNC(SYSDATE)
              AND    TRIM(b.cfeadc) = 'HP'
              AND    TRIM(b.cfatyp) = 'C'
              AND    b.cfaccn BETWEEN g_min_count AND g_max_count;
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

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    SELECT MAX(a.cfaccn)
    INTO   g_cif_count
    FROM   sync_cfconn a;
    SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO bk_cif c
            USING (SELECT TRIM(sc.cfaccn) cfaccn,
                          TRIM(sc.cfeadd) addr
                   FROM   sync_cfconn sc
                   /*WHERE  sc.cfeadc = 'HP'
                   AND    sc.cfatyp = 'C'*/
                   WHERE  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
                                       FROM   sync_cfconn sc2
                                       WHERE  sc2.cfaccn = sc.cfaccn
                                       AND    sc2.cfatyp = sc.cfatyp
                                       AND    sc2.cfeadc = sc.cfeadc)
                   AND    LENGTH(rtrim(sc.cfeadd)) < 30
                   AND sc.cfaccn BETWEEN g_min_count AND g_max_count
                   --AND    sc.cfaccn = cif_tab(i)
                   ) a
            ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
            WHEN MATCHED THEN
              UPDATE
              SET    c.telephone = a.addr;
          COMMIT;

          g_min_count := g_max_count;

                --khong co them ban ghi nao
              EXIT WHEN(g_max_count > g_cif_count);

             EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;
                IF (g_error_count >= 10)
                THEN
                  RAISE;
                END IF;
          END;
        END LOOP;
        --noformat end

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;
    SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            --Update 2011/07/30, also sync to bc_user_info
--            MERGE INTO bc_user_info c
--            USING (SELECT t2.cfaccn,
--                          TRIM(t2.cfeadd) addr
--                   FROM   (SELECT b.cfaccn,
--                                  MAX(b.cfzseq) seq
--                           FROM   sync_cfconn b
--                           GROUP  BY b.cfaccn) t1,
--                          sync_cfconn t2
--                   WHERE  t1.cfaccn = t2.cfaccn
--                   AND    t1.seq = t2.cfzseq
--                         /*AND    t2.cfeadc = 'HP'
--                         AND    t2.cfatyp = 'C'*/
--                   AND    LENGTH(rtrim(t2.cfeadd)) < 30
--                   AND t1.cfaccn BETWEEN g_min_count AND g_max_count
--                   --AND    t1.cfaccn = cif_tab(i)
--                   ) a
--            ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
--            WHEN MATCHED THEN
--              UPDATE
--              SET    c.telephone = a.addr;
--          COMMIT;

          g_min_count := g_max_count;

                --khong co them ban ghi nao
              EXIT WHEN(g_max_count > g_cif_count);

             EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;
                IF (g_error_count >= 10)
                THEN
                  RAISE;
                END IF;
          END;
        END LOOP;
        --noformat end

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_telephone_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_telephone_sync',
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
  PROCEDURE proc_cif_info_sync IS
  BEGIN
    v_start_date      := SYSDATE;
    g_loop_count      := 10;
    g_limit_count     := 200000;
    g_min_count       := 0;
    g_max_count       := 0;
    g_cif_count       := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

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
              FROM   STG.SI_DAT_CFMAST@STAGING_PRO a
              WHERE  a.cfcifn >= g_min_count AND a.cfcifn < g_max_count;
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

    g_min_count      := 0;
    g_max_count      := 0;
    g_sub_loop_count := 0;
    g_error_count    := 0;

    g_error_level := 2;
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
            INSERT INTO sync_cfaddr
              SELECT a.cfcifn,
                     a.cfadsq,
                     a.cfna2
              FROM   svdatpv51.cfaddr@dblink_data a
              WHERE  a.cfcifn >= g_min_count AND a.cfcifn < g_max_count;
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

    g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 100000;

    /*SELECT MAX(a.cfcifn)
    INTO   g_cif_count
    FROM   sync_cfmast a;*/

    SELECT MAX(a.cfcifn)
    INTO   g_cif_count
    FROM   sync_cfmast a;
    SELECT MIN(a.cfcifn)
    INTO   g_min_count
    FROM   sync_cfmast a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start        
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

    		g_error_level := 5;
            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO bk_cif c
            USING (SELECT /*+ INDEX(sync_cfmast, IDX_SYNC_CFMAST) */
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
                    /*ADD for taxcode*/
                    a.taxcod,
                    b.cfna2
                   FROM   sync_cfmast a,
                          sync_cfaddr b
                   WHERE  length(rtrim(a.cfssno)) < 40 --fix length of char db2 always max
                   AND    b.cfadsq = (SELECT MAX(c.cfadsq)
                                      FROM   sync_cfaddr c
                                      WHERE  c.cfcifn = b.cfcifn)
                   AND    a.cfcifn = b.cfcifn
                   AND    a.cfcifn >= g_min_count AND a.cfcifn < g_max_count
                   --AND    a.cfcifn = cif_tab(i)
                   --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                   ) a
            ON (a.cfcifn = c.cif_no)
            WHEN MATCHED THEN
              UPDATE
              SET    c.cert_type     = TRIM(a.cfsscd),
                     c.cert_code     = TRIM(a.cfssno),
                     c.bank_no       = '302' /* LPAD(TRIM(a.cfbnkn),
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      3,
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      '0') */,
                     c.org_no        = (case when length(TRIM(a.cfbrnn)) <= 2 then LPAD (TRIM(a.cfbrnn), 3, '0') else TO_CHAR(TRIM(a.cfbrnn)) end),
                     c.cif_acct_name = TRIM(a.cfna1),
                     c.birth_date    = DECODE(LENGTH(a.cfbird),
                                              7,
                                              TO_DATE(a.cfbird,
                                                      'yyyyddd'),
                                              NULL),
                     c.birth_place   = TRIM(a.cfbirp),
                     c.country       = TRIM(a.cfcitz),
                     c.individual    = TRIM(a.cfindi),
                     c.taxcod        = TRIM(a.taxcod),
                     c.addr          = a.cfna2;
        /*  WHEN NOT MATCHED
        THEN
          INSERT
            (
              c.cif_no,
              c.cert_type,
              c.cert_code,
              c.bank_no,
              c.org_no,
              c.cif_acct_name,
              c.birth_place,
              c.country,
              c.individual,
              c.id,
              c.sync_hist
            )
          VALUES
            (
              TRIM(a.cfcifn),
              TRIM(a.cfsscd),
              TRIM(a.cfssno),
              --trim(a.cfbnkn),
              '302',
              LPAD(TRIM(a.cfbrnn), 3, 0),
              TRIM(a.cfna1),
              TRIM(a.cfbirp),
              TRIM(a.cfcitz),
              TRIM(a.cfindi),
              0,
              1
            ); */

    		COMMIT;


    		g_error_level := 6;
            MERGE INTO   bb_corp_info c
                     USING   (SELECT /*+ INDEX(sync_cfmast, IDX_SYNC_CFMAST) */
                                    a  .cfcifn,
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
                                       /*ADD for taxcode*/
                                       a.taxcod,
                                       b.cfna2
                                FROM   sync_cfmast a, sync_cfaddr b
                               WHERE   LENGTH (RTRIM (a.cfssno)) < 40 --fix length of char db2 always max
                                       AND b.cfadsq =
                                              (SELECT   MAX (c.cfadsq)
                                                 FROM   sync_cfaddr c
                                                WHERE   c.cfcifn = b.cfcifn)
                                       AND a.cfcifn = b.cfcifn
                                       AND a.cfcifn >= g_min_count
                                       AND a.cfcifn < g_max_count
                             ) a
                        ON   (a.cfcifn = c.cif_no AND c.status = 'ACTV')
                WHEN MATCHED
                THEN
                    UPDATE SET c.cert_type = TRIM (a.cfsscd),
                               c.cert_code = TRIM (a.cfssno),
                               c.cif_acct_name = TRIM (a.cfna1),
                               c.address = a.cfna2;

           COMMIT;

           g_min_count := g_max_count;

              --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);

           EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_merge_bulk;
              g_error_count := g_error_count + 1;
              IF (g_error_count >= 10)
              THEN
                RAISE;
              END IF;
        END;
        END LOOP;
        --noformat end

    /*g_error_level := 4;

    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    SELECT MIN(a.cfcifn)
    INTO   g_min_count
    FROM   sync_cfmast a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;

    --noformat start
        g_error_level := 5;
        LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;

            \*SELECT \*+ ALL_ROWS *\
             a.cfcifn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfmast a
            WHERE  a.cfcifn BETWEEN g_min_count AND g_max_count;*\

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO bc_user_info c
            USING (SELECT \*+ INDEX(sync_cfmast, IDX_SYNC_CFMAST) *\
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
                    b.cfna2
                   FROM   sync_cfmast a,
                          sync_cfaddr b
                   WHERE  length(rtrim(a.cfssno)) < 40 --fix length of char db2 always max
                   AND    b.cfadsq = (SELECT MAX(c.cfadsq)
                                      FROM   sync_cfaddr c
                                      WHERE  c.cfcifn = b.cfcifn)
                   AND    a.cfcifn = b.cfcifn
                   AND    a.cfcifn BETWEEN g_min_count AND g_max_count
                   --AND    a.cfcifn = cif_tab(i)
                   --AND    a.ROWID BETWEEN min_rid_tab(i) AND max_rid_tab(i)
                   ) a
            ON (to_char(a.cfcifn) = c.cif_no AND c.status = 'ACTV')
            WHEN MATCHED THEN
              UPDATE
              SET    c.cert_type = TRIM(a.cfsscd),
                     c.cert_code = TRIM(a.cfssno),
                     --c.bank_no = TRIM(a.cfbnkn),
                     --c.org_no = TRIM(a.cfbrnn),
                     c.cif_acct_name = TRIM(a.cfna1),
                     c.address       = a.cfna2;
        --c.birth_date = DECODE(LENGTH(a.cfbird), 7, TO_DATE(a.cfbird,'yyyyddd'), NULL),
        --c.birth_place = TRIM(a.cfbirp),
        --c.country = TRIM(a.cfcitz),
        --c.individual = TRIM(a.cfindi);

          COMMIT;

      g_min_count := g_max_count;

                --khong co them ban ghi nao
              EXIT WHEN(g_max_count > g_cif_count);

             EXCEPTION
              WHEN OTHERS THEN
                ROLLBACK TO s_merge_bulk;
                g_error_count := g_error_count + 1;
                IF (g_error_count >= 10)
                THEN
                  RAISE;
                END IF;
          END;
        END LOOP;
    --noformat end*/

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_CIF_INFO');

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_cif_info_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_CIF_INFO');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_cif_info_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_all_address_cif_sync IS
    v_checkpoint_date NUMBER;
    v_checkpoint_time NUMBER;
  BEGIN
    v_start_date := SYSDATE;

    g_loop_count      := 10;
    g_limit_count     := 200000;
    g_min_count       := 0;
    g_max_count       := 0;
    g_cif_count       := 0;
    g_error_count     := 0;
    g_error_sub_count := 0;
    g_sub_loop_count  := 0;
    v_checkpoint_date := 0;
    v_checkpoint_time := 0;

    SELECT a.sync_count
    INTO   g_cif_count
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CFMAST';

    g_cif_count := g_cif_count + 1000;

    SELECT to_number(to_char(a.sync_end_time,
                             'yyyy') || to_char(a.sync_end_time,
                                                'ddd'))
    INTO   v_checkpoint_date
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CIFADD';

    SELECT a.sync_count
    INTO   v_checkpoint_time
    FROM   sync_checkpoint a
    WHERE  a.sync_type = 'CIFADD';

    g_error_level := 1;
    FOR i IN 1 .. g_loop_count
    LOOP
      -- try g_loop_count times
      BEGIN
        g_min_count       := 0;
        g_error_sub_count := 0;
        SAVEPOINT s_insert_temp;

        /*LOOP
        BEGIN
          SAVEPOINT s_intert_temp_l;

          g_min_count := g_max_count;
          g_max_count := g_min_count + g_limit_count;*/

        INSERT INTO sync_cfconn
          SELECT /*+ ALL_ROWS */
           b.cfaccn,
           b.cfeadd,
           b.cfeadc,
           b.cfzseq,
           b.cfatyp,
           b.cfcifn,
           b.cfatim
          FROM   svdatpv51.cfconn@DBLINK_DATA b
          WHERE  b.cfadlm <> 0
                --AND    b.cfatim <> 235959 --trong moi truong test thoi
          AND    b.cfatim >= v_checkpoint_time
          AND    b.cfadlm >= v_checkpoint_date
          AND    (b.cfeadc = 'MP' OR b.cfeadc = 'EM')
          /*AND    b.cfatyp = 'C'*/
          ;
        --AND    length(rtrim(b.cfeadd)) < 21
        --AND    b.cfaccn BETWEEN g_min_count AND g_max_count;

        COMMIT;
        --khong co them ban ghi nao
        /*EXIT WHEN(g_max_count > g_cif_count);
          EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_insert_temp_l;
              g_error_sub_count := g_error_sub_count + 1;
              g_min_count       := g_min_count - g_limit_count;
              g_max_count       := g_max_count - g_limit_count;
              IF (g_error_sub_count > 10)
              THEN
                RAISE;
              END IF;
              --DBMS_LOCK.SLEEP(10);
          END;
        END LOOP;*/
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


    g_error_level := 4;
    /*
    g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 100000;*/

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    /*SELECT MAX(a.cfaccn)
    INTO   g_cif_count
    FROM   sync_cfconn a;
    SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;*/

    --noformat start
        g_error_level := 5;
        /*LOOP
          BEGIN
            SAVEPOINT s_merge_bulk;

            g_max_count := g_min_count + g_limit_count;*/

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
            MERGE INTO bk_cif c
                    USING (SELECT TRIM(sc.cfaccn) cfaccn,
                                                TRIM(sc.cfeadd) addr
                                 FROM   sync_cfconn sc
                                 WHERE  sc.cfeadc = 'EM'
                   AND    sc.cfatyp = 'C'
                                 AND  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
                                                                         FROM   sync_cfconn sc2
                                                                         WHERE  sc2.cfaccn = sc.cfaccn
                                                                         AND    sc2.cfatyp = sc.cfatyp
                                                                         AND    sc2.cfeadc = sc.cfeadc)
                                 --AND sc.cfaccn BETWEEN g_min_count AND g_max_count
                                 --AND    sc.cfaccn = cif_tab(i)
                                ) a
                    ON (a.cfaccn = c.cif_no)
                    WHEN MATCHED THEN
                        UPDATE
                        SET    c.email  = DECODE(a.addr, NULL, c.email, a.addr);

              COMMIT;


    MERGE INTO   bb_corp_info c
             USING   (SELECT   TRIM (sc.cfaccn) cfaccn, TRIM (sc.cfeadd) addr
                        FROM   sync_cfconn sc
                       WHERE   sc.cfeadc = 'EM' AND sc.cfatyp = 'C'
                               AND sc.cfzseq =
                                      (SELECT   MAX (sc2.cfzseq)
                                         FROM   sync_cfconn sc2
                                        WHERE       sc2.cfaccn = sc.cfaccn
                                                AND sc2.cfatyp = sc.cfatyp
                                                AND sc2.cfeadc = sc.cfeadc) --AND sc.cfaccn BETWEEN g_min_count AND g_max_count
                                                                           --AND    sc.cfaccn = cif_tab(i)
                     )
                     a
                ON   (a.cfaccn = c.cif_no AND c.status = 'ACTV')
        WHEN MATCHED
        THEN
            UPDATE SET c.email = DECODE (a.addr, NULL, c.email, a.addr);

        COMMIT;

--            MERGE INTO bc_user_info c
--                    USING (SELECT TRIM(sc.cfaccn) cfaccn,
--                                                TRIM(sc.cfeadd) addr
--                                 FROM   sync_cfconn sc
--                                 WHERE  sc.cfeadc = 'EM'
--                   AND    sc.cfatyp = 'C'
--                                 AND  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
--                                                                         FROM   sync_cfconn sc2
--                                                                         WHERE  sc2.cfaccn = sc.cfaccn
--                                                                         AND    sc2.cfatyp = sc.cfatyp
--                                                                         AND    sc2.cfeadc = sc.cfeadc)
--                                 --AND sc.cfaccn BETWEEN g_min_count AND g_max_count
--                                 --AND    sc.cfaccn = cif_tab(i)
--                                ) a
--                    ON (a.cfaccn = c.cif_no)
--                    WHEN MATCHED THEN
--                        UPDATE
--                        SET    c.email  = DECODE(a.addr, NULL, c.email, a.addr);
--    
--            COMMIT;


        MERGE INTO   bb_user_info c
             USING   (SELECT   TRIM (sc.cfaccn) cfaccn,
                               TRIM (sc.cfeadd) addr,
                               b.corp_id
                        FROM   sync_cfconn sc, bb_corp_info b
                       WHERE       sc.cfaccn = b.cif_no
                               AND sc.cfeadc = 'EM'
                               AND sc.cfatyp = 'C'
                               AND sc.cfzseq =
                                      (SELECT   MAX (sc2.cfzseq)
                                         FROM   sync_cfconn sc2
                                        WHERE       sc2.cfaccn = sc.cfaccn
                                                AND sc2.cfatyp = sc.cfatyp
                                                AND sc2.cfeadc = sc.cfeadc) --AND sc.cfaccn BETWEEN g_min_count AND g_max_count
                                                                           --AND    sc.cfaccn = cif_tab(i)
                     )
                     a
                ON   (a.corp_id = c.corp_id AND c.status = 'ACTV')
        WHEN MATCHED
        THEN
            UPDATE SET c.email = DECODE (a.addr, NULL, c.email, a.addr);

        COMMIT;

              /*g_min_count := g_max_count;

              --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);

           EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_merge_bulk;
              g_error_count := g_error_count + 1;
              IF (g_error_count > 10)
              THEN
                RAISE;
              END IF;
        END;
        END LOOP;*/
        --noformat end

    /*g_error_count := 0;
    g_min_count   := 0;
    g_max_count   := 0;
    g_limit_count := 10000;*/

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;

    --SELECT MAX(a.cfaccn) INTO g_cif_count FROM sync_cfconn a;
    /*SELECT MIN(a.cfaccn)
    INTO   g_min_count
    FROM   sync_cfconn a;

    IF (g_cif_count IS NULL)
    THEN
      g_cif_count := 0;
    END IF;

    IF (g_min_count IS NULL)
    THEN
      g_min_count := 0;
    END IF;*/

    --noformat start
        g_error_level := 5;
        /*LOOP
          BEGIN
            SAVEPOINT s_merge_bulk_1;

            g_max_count := g_min_count + g_limit_count;*/

            /*SELECT \*+ ALL_ROWS *\
             a.cfaccn BULK COLLECT
            INTO   cif_tab
            FROM   sync_cfconn a
            WHERE  a.cfaccn BETWEEN g_min_count AND g_max_count;*/

            --FORALL i IN cif_tab.FIRST .. cif_tab.LAST
              --Update 2011/07/30, also sync to bc_user_info
              MERGE INTO bk_cif c
                USING (SELECT TRIM(sc.cfaccn) cfaccn,
                                            TRIM(sc.cfeadd) addr
                             FROM   sync_cfconn sc
                             WHERE  sc.cfeadc = 'MP'
                             AND    sc.cfatyp = 'C'
                             AND    length(rtrim(sc.cfeadd)) < 21
                             AND  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
                                                                     FROM   sync_cfconn sc2
                                                                     WHERE  sc2.cfaccn = sc.cfaccn
                                                                     AND    sc2.cfatyp = sc.cfatyp
                                                                     AND    sc2.cfeadc = sc.cfeadc)
                            --AND  sc.cfaccn BETWEEN g_min_count AND g_max_count
                            --AND    sc.cfaccn = cif_tab(i)
                            ) a
                ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
                WHEN MATCHED THEN
                    UPDATE
                    SET    c.mobile = DECODE(a.addr, NULL, c.mobile, a.addr);

                            COMMIT;


        MERGE INTO   bb_corp_info c
             USING   (SELECT   TRIM (sc.cfaccn) cfaccn, TRIM (sc.cfeadd) addr
                        FROM   sync_cfconn sc
                       WHERE       sc.cfeadc = 'MP'
                               AND sc.cfatyp = 'C'
                               AND LENGTH (RTRIM (sc.cfeadd)) < 21
                               AND sc.cfzseq =
                                      (SELECT   MAX (sc2.cfzseq)
                                         FROM   sync_cfconn sc2
                                        WHERE       sc2.cfaccn = sc.cfaccn
                                                AND sc2.cfatyp = sc.cfatyp
                                                AND sc2.cfeadc = sc.cfeadc) --AND  sc.cfaccn BETWEEN g_min_count AND g_max_count
                                                                           --AND    sc.cfaccn = cif_tab(i)
                     )
                     a
                ON   (a.cfaccn = c.cif_no AND a.addr IS NOT NULL AND c.status = 'ACTV')
        WHEN MATCHED
        THEN
            UPDATE SET c.mobile = DECODE (a.addr, NULL, c.mobile, a.addr);

        COMMIT;

--                MERGE INTO bc_user_info c
--              USING (SELECT TRIM(sc.cfaccn) cfaccn,
--                            TRIM(sc.cfeadd) addr
--                     FROM   sync_cfconn sc
--                     WHERE  sc.cfeadc = 'MP'
--                     AND    sc.cfatyp = 'C'
--                             AND    length(rtrim(sc.cfeadd)) < 21
--                     AND  sc.cfzseq = (SELECT MAX(sc2.cfzseq)
--                                         FROM   sync_cfconn sc2
--                                         WHERE  sc2.cfaccn = sc.cfaccn
--                                         AND    sc2.cfatyp = sc.cfatyp
--                                         AND    sc2.cfeadc = sc.cfeadc)
--                    --AND sc.cfaccn BETWEEN g_min_count AND g_max_count
--                                    --AND    sc.cfaccn = cif_tab(i)
--                    ) a
--              ON (a.cfaccn = c.cif_no AND a.addr IS NOT NULL)
--              WHEN MATCHED THEN
--                UPDATE
--                SET    c.mobile = DECODE(a.addr, NULL, c.mobile, a.addr);
--    
--                            COMMIT;

        MERGE INTO   bb_user_info c
             USING   (SELECT   TRIM (sc.cfaccn) cfaccn,
                               TRIM (sc.cfeadd) addr,
                               b.corp_id
                        FROM   sync_cfconn sc, bb_corp_info b
                       WHERE       sc.cfaccn = b.cif_no
                               AND sc.cfeadc = 'MP'
                               AND sc.cfatyp = 'C'
                               AND LENGTH (RTRIM (sc.cfeadd)) < 21
                               AND sc.cfzseq =
                                      (SELECT   MAX (sc2.cfzseq)
                                         FROM   sync_cfconn sc2
                                        WHERE       sc2.cfaccn = sc.cfaccn
                                                AND sc2.cfatyp = sc.cfatyp
                                                AND sc2.cfeadc = sc.cfeadc) --AND sc.cfaccn BETWEEN g_min_count AND g_max_count
                                                                           --AND    sc.cfaccn = cif_tab(i)
                     )
                     a
                ON   (    a.corp_id = c.corp_id
                      AND a.addr IS NOT NULL
                      AND c.status = 'ACTV')
        WHEN MATCHED
        THEN
            UPDATE SET c.mobile = DECODE (a.addr, NULL, c.mobile, a.addr);

        COMMIT;

              /*g_min_count := g_max_count;

              --khong co them ban ghi nao
            EXIT WHEN(g_max_count > g_cif_count);

           EXCEPTION
            WHEN OTHERS THEN
              ROLLBACK TO s_merge_bulk_1;
              g_error_count := g_error_count + 1;
              IF (g_error_count > 10)
              THEN
                RAISE;
              END IF;
        END;
        END LOOP;*/
        --noformat end

    MERGE INTO sync_checkpoint a
    USING (SELECT MAX(sd.cfatim) end_time
           FROM   sync_cfconn sd) src
    ON (a.sync_type = 'CIFADD' AND src.end_time IS NOT NULL)
    WHEN MATCHED THEN
      UPDATE
      SET    a.sync_count    = src.end_time,
             a.sync_end_time = SYSDATE,
             a.is_sync       = 'Y';

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link DBLINK_DATA';

    ebank_common_sync.proc_update_status_job_sync('Y',
                                                  'SYNC_CIF_ADDRESS');

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_all_address_cif_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;

      ebank_common_sync.proc_update_status_job_sync('N',
                                                    'SYNC_CIF_ADDRESS');

      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_all_address_cif_sync',
                                    --'SYSTEM BUSY'
                                    SUBSTR(TO_CHAR(g_error_level) ||
                                           'Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;


  PROCEDURE proc_reset_checkpoint IS
  BEGIN
    UPDATE sync_checkpoint a
    SET    a.sync_end_time = SYSDATE,
           a.sync_count    = 0
    WHERE  a.sync_type = 'CIFADD';
    COMMIT;

    ebank_sync_util.proc_sync_log(SYSDATE,
                                  SYSDATE,
                                  'proc_reset_checkpoint',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(SYSDATE,
                                    SYSDATE,
                                    'proc_reset_checkpoint',
                                    --'SYSTEM BUSY'
                                    SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
                                           SQLERRM,
                                           1,
                                           1000),
                                    'FAIL');
  END;

--------------------------------------------------------------------------------
END;

/
