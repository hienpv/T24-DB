--------------------------------------------------------
--  DDL for Package Body CSPKG_CLEAR
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."CSPKG_CLEAR" 
  IS
 /*----------------------------------------------------------------------------------------------------
     ** Module   : ODS SYSTEM
     ** and is copyrighted by FSS.
     **
     **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
     **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
     **    graphic, optic recording or otherwise, translated in any language or computer language,
     **    without the prior written permission of Financial Software Solutions. JSC.
     **
     **  MODIFICATION HISTORY
     **  Person      Date           Comments
     **  LocTX      02-12-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/
    pkgctx    plog.log_ctx;
    logrow    tlogdebug%ROWTYPE;


/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  xoa tran map dong bo trong ngay
 **  Person      Date           Comments
 **  LocTX     05/12/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_delete_cb_cdc(p_etl_date NUMBER)
    IS
        l_error_desc VARCHAR2(300);
        l_etl_date DATE;
        l_check_exists number;
    BEGIN
        plog.setbeginsection (pkgctx, 'pr_cb_cdc_delete');

        DELETE FROM sync_cdc_tmtran a
        WHERE a.tmentdt7 <= TO_NUMBER(TO_CHAR(TO_DATE(p_etl_date, 'RRRRDDD') - 2, 'RRRRDDD'));

        COMMIT;
        /*
        -- commend trien khai tmtran24 
        DELETE FROM bk_account_history_temp a
          WHERE a.TRAN_TIME <= TO_NUMBER(TO_CHAR(TO_DATE(p_etl_date, 'RRRRDDD') - 1, 'RRRRDDD'));
        COMMIT;  
        */
/*-- lay db link sang rawstage, ko can dong bo ve
       -- DELETE FROM sync_cdc_ddmemo a
       -- WHERE a.dla7 <= p_etl_date;

        EXECUTE IMMEDIATE ' TRUNCATE TABLE SYNC_CDC_CDMEMO'; --Khong can do capture truc tiep
        EXECUTE IMMEDIATE ' TRUNCATE TABLE SYNC_CDC_LNMEMO';--todo: kiem tra lai

        COMMIT;
*/
        l_etl_date := TO_DATE(p_etl_date, 'RRRRDDD');
        DELETE sync_cdc_tranmap a
        WHERE TRUNC(TO_DATE(a.message_date, 'yyyymmddhh24mi')) <= l_etl_date;
        COMMIT;
--/*
        DELETE FROM sync_cdc_ibps_msg_content A--#20141216 Loctx add
        WHERE a.trans_date <= l_etl_date - 45;
--*/
        DELETE FROM sync_cdc_vcb_msg_content A--#20141216 Loctx add
        WHERE a.trans_date <= l_etl_date - 45;

        DELETE FROM sync_cdc_swift_msg_content A--#20141216 Loctx add
        WHERE a.trans_date <= l_etl_date - 45;

        DELETE FROM sync_cdc_bk_acct_hist_fail
        WHERE process_ind = 'Y';

        DELETE FROM sync_cdc_rmmast--#20150412 Loctx add
        WHERE RMEND7 < TO_NUMBER(TO_CHAR(TO_DATE(p_etl_date, 'RRRRDDD') - 45, 'RRRRDDD'));
        COMMIT;

        DELETE FROM sync_cdc_rmdetl--#20150418 Loctx: disable action xoa cua CDC va thuc hien xoa thu cong
        WHERE RDJEFF < TO_NUMBER(TO_CHAR(TO_DATE(p_etl_date, 'RRRRDDD') - 45, 'RRRRDDD'));
        
        delete  cstb_cdc_log a 
        where a.log_date  <  l_etl_date - 10  ;
        
        COMMIT;

        --#20150811 LocTX add
        --#20150627 Loctx add for clear
        SELECT (CASE WHEN EXISTS( SELECT 1 FROM cstb_data_later_process WHERE insert_date7 = p_etl_date)
                    THEN 1
                    ELSE 0
                    END)
        INTO l_check_exists
        FROM DUAL;

        IF l_check_exists > 0 THEN

            INSERT INTO ibs.cstb_data_later_process_hist
            (
                insert_date7, insert_date, table_name, store_procedure,
                process_ind, process_date, process_count
            )
            SELECT a.insert_date7, a.insert_date, a.table_name, a.store_procedure,
                   a.process_ind, a.process_date, a.process_count
              FROM cstb_data_later_process a
            WHERE insert_date7 <= p_etl_date;

            DELETE FROM cstb_data_later_process WHERE insert_date7 <= p_etl_date; --#20161223 Loctx change from insert_date7 = p_etl_date
            COMMIT;

        END IF;

        plog.setendsection( pkgctx, 'pr_cb_cdc_delete' );
    EXCEPTION
    WHEN OTHERS
    THEN
        ROLLBACK;
        l_error_desc := substr(SQLERRM, 200);
        plog.error( pkgctx, l_error_desc);

        plog.setendsection( pkgctx, 'pr_cb_cdc_delete' );
        RAISE;

    END;


/*----------------------------------------------------------------------------------------------------
 ** Description:
 **  xoa tran map dong bo trong ngay
 **  Person      Date           Comments
 **  LocTX     04/11/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_delete_tranmap(p_etl_date NUMBER)
    IS
        l_etl_date DATE;
        l_error_desc VARCHAR2(300);
    BEGIN
        plog.setbeginsection (pkgctx, 'pr_delete_tranmap');
        l_etl_date := TO_DATE(p_etl_date, 'RRRRDDD');



        plog.setendsection( pkgctx, 'pr_delete_tranmap' );

    EXCEPTION
    WHEN OTHERS
    THEN
        ROLLBACK;
        l_error_desc := substr(SQLERRM, 200);
        plog.error( pkgctx, l_error_desc);

        plog.setendsection( pkgctx, 'pr_delete_tranmap' );
        RAISE;

    END;

BEGIN
    SELECT   *
    INTO   logrow
    FROM   tlogdebug
    WHERE   ROWNUM <= 1;

    pkgctx      :=
    plog.init('cspkg_clear',
        plevel => logrow.loglevel,
        plogtable => ( logrow.log4table = 'Y' ),
        palert => ( logrow.log4alert = 'Y' ),
        ptrace => ( logrow.log4trace = 'Y' ) );
END;

/
