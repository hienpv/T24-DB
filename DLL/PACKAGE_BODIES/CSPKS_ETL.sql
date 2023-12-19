--------------------------------------------------------
--  DDL for Package Body CSPKS_ETL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKS_ETL" 
IS
    pkgctx   plog.log_ctx;
    logrow   tlogdebug%ROWTYPE;
/*----------------------------------------------------------------------------------------------------
 ** Module: FTP SYSTEM
 ** and is copyrighted by FSS.
 **
 **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
 **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
 **    graphic, optic recording or otherwise, translated in any language or computer language,
 **    without the prior written permission of Financial Software Solutions. JSC.
 **
 **  MODIFICATION HISTORY
 **  Person            Date                Comments
 **  Loctx            22/08/2013               Created
 **
 ** (c) 2013 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_main_etl_begin(
                            p_user             IN VARCHAR2,
                            p_job_name    IN VARCHAR2,
                            p_etl_date         IN NUMBER,
                            p_process_id       OUT VARCHAR2
                            )
    IS
        l_return NUMBER := cspkg_errnums.c_success;
        l_process_id VARCHAR2(100);
        l_execute_ref_no    VARCHAR2(100);

        l_error VARCHAR2(10);

    BEGIN
        plog.setbeginsection (pkgctx, 'pr_etl_begin');
        IF cspks_util.fn_get_seq (p_job_name,
                                  TO_CHAR (SYSDATE, 'MMDD'),
                                  l_execute_ref_no,
                                  4,
                                  l_error) <> 1
        THEN
            RAISE cspkg_errnums.e_system_error;
        END IF;

        l_process_id := p_job_name || l_execute_ref_no;

        INSERT INTO cstb_etl_log(process_id, process_name, process_group, param_data,
                                        status, start_date,
                                        maker_id)
                    VALUES(
                        l_process_id, p_job_name, NULL, p_etl_date,
                        'O', SYSDATE, p_user
                    );


        p_process_id := l_process_id;
        plog.setendsection (pkgctx, 'pr_etl_begin');
    EXCEPTION
        WHEN cspkg_errnums.e_system_error
        THEN
            ROLLBACK;
            plog.error (pkgctx, 'got error:' || l_error);
            plog.setendsection (pkgctx, 'pr_etl_begin');
            RAISE  cspkg_errnums.e_system_error;
        WHEN OTHERS
        THEN
            plog.error (pkgctx, SQLERRM);
            plog.setendsection (pkgctx, 'pr_etl_begin');
            RAISE;
    END;
    
    PROCEDURE prc_gather_table (p_n_tream_run number)
   IS 
    BEGIN
        
        -- Truoc gather
        
        -- cap nhat bangr moi tren staging   
            merge into  stg_gather_table a
            using (
                   select distinct owner, table_name,NUM_ROWS 
                   from DBA_TABLES  
                   where owner ='IBS'  
                   ) b
            ON (a.owner =b.owner and a.n_table =b.table_name and a.status =1)

            WHEN  MATCHED THEN 
               update set RN_bef_gather = b.NUM_ROWS ;
       
            commit; 
      
        for rown in (select * from IBS.stg_gather_table where status = 1 and n_tream_run = p_n_tream_run ) 
        loop
            begin 
                  DBMS_STATS.GATHER_TABLE_STATS (ownname =>rown.owner , tabname => rown.n_table, degree =>10);
            end;
            
        End loop;
        
        -- sau gather
          merge into  stg_gather_table a
            using (
                   select distinct owner, table_name,NUM_ROWS 
                   from DBA_TABLES  
                   where owner ='IBS' 
                   ) b
            ON (a.owner =b.owner and a.n_table =b.table_name and a.status =1)

            WHEN  MATCHED THEN 
               update set RN_AFT_GATHER = b.NUM_ROWS ;
       
            commit; 
     
     EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
        WHEN OTHERS THEN
            -- Consider logging the error and then re-raise
            RAISE;
                
     END;

    PROCEDURE pr_etl_begin( p_user             IN VARCHAR2,
                            p_job_name        IN VARCHAR2,
                            p_process_group     IN VARCHAR2,
                            p_process_id       IN VARCHAR2,
                            p_etl_date         IN NUMBER
                            )

    IS
        l_return NUMBER := cspkg_errnums.c_success;

        l_error VARCHAR2(10);

    BEGIN
        plog.setbeginsection (pkgctx, 'pr_etl_begin');

        INSERT INTO cstb_etl_log(process_id, process_name, process_group, param_data,
                                        status, start_date,
                                        maker_id)
                    VALUES(
                        p_process_id, p_job_name, p_process_group, p_etl_date,
                        'O', sysdate, p_user
                    );


        plog.setendsection (pkgctx, 'pr_etl_begin');
    EXCEPTION
        WHEN cspkg_errnums.e_system_error
        THEN
            ROLLBACK;
            plog.error (pkgctx, 'got error:' || l_error);
            plog.setendsection (pkgctx, 'pr_etl_begin');
            RAISE  cspkg_errnums.e_system_error;
        WHEN OTHERS
        THEN
            plog.error (pkgctx, SQLERRM);
            plog.setendsection (pkgctx, 'pr_etl_begin');
            RAISE  cspkg_errnums.e_system_error;
    END;

    PROCEDURE pr_etl_end(   p_job_name       IN VARCHAR2,
                            p_process_id       IN VARCHAR2,
                            p_success_ind    IN CHAR,
                            p_error         IN VARCHAR2,
                            p_description  IN VARCHAR2
                            )

    IS
        l_error VARCHAR2(10);
        l_status CHAR(1);
        l_description VARCHAR2(200);
    BEGIN
        plog.setbeginsection (pkgctx, 'pr_etl_end');
        --plog.debug(pkgctx, 'p_success_ind=' ||p_success_ind );
        IF p_success_ind = 'Y' THEN
            l_description := 'sucesssfull';
            l_status := cspkg_errnums.c_process_success;
        ELSE
            l_description :=  p_description;
            l_status := cspkg_errnums.c_process_fail;
        END IF;

        --log process

        UPDATE cstb_etl_log SET status = l_status,
                                    error_code = p_error,
                                    description = l_description,
                                    stop_date = SYSDATE
        WHERE process_id = p_process_id AND process_name = p_job_name;

        COMMIT;

        plog.setendsection (pkgctx, 'pr_etl_end');

        IF p_success_ind <> 'Y' THEN
            RAISE cspkg_errnums.e_system_error;
        END IF;

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            plog.error (pkgctx, SUBSTR(SQLERRM,1,250));
            plog.setendsection (pkgctx, 'pr_etl_end');
            RAISE;
    END;

BEGIN
    SELECT   *
      INTO   logrow
      FROM   tlogdebug
     WHERE   ROWNUM <= 1;

    pkgctx :=
        plog.init ('cspks_etl',
                   plevel      => logrow.loglevel,
                   plogtable   => (logrow.log4table = 'Y'),
                   palert      => (logrow.log4alert = 'Y'),
                   ptrace      => (logrow.log4trace = 'Y'));
-- Enter further code below as specified in the Package spec.
END;

/
