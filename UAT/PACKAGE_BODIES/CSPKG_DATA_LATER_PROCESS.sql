--------------------------------------------------------
--  DDL for Package Body CSPKG_DATA_LATER_PROCESS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."CSPKG_DATA_LATER_PROCESS" 
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
 **  LocTX      23/06/2015    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/
    pkgctx    plog.log_ctx;
    logrow    tlogdebug%ROWTYPE;

    PROCEDURE pr_data_later_process
    IS
        CURSOR c_data (p_cur_date7 NUMBER)
        IS
            SELECT rowid rid, a.*
            FROM cstb_data_later_process a
            WHERE process_ind <> 'Y'
            AND process_count < 5
            --AND insert_date7 = p_cur_date7--#20161223 Loctx disable
            --AND store_procedure LIKE 'txpkg_transaction_cdc.fn_tmtra%'
            ORDER BY insert_date;

        TYPE ty_data_tb IS TABLE OF c_data%ROWTYPE;

        l_data_list ty_data_tb;

        l_error_desc VARCHAR2(500);
        l_cur_date7 NUMBER(7);

        l_Return NUMBER;

    BEGIN
        plog.setbeginsection( pkgctx, 'pr_data_later_process' );

        plog.debug(pkgctx, 'start ');

        SELECT param_value
        INTO l_cur_date7
        FROM cstb_system a
        WHERE param_name = 'CUR_DATE7';


        OPEN c_data(l_cur_date7);
        LOOP
            FETCH c_data
            BULK COLLECT INTO l_data_list
            LIMIT  1000;
            plog.debug(pkgctx, 'l_data_list.COUNT=' || l_data_list.COUNT);

            FOR idx IN 1..l_data_list.count LOOP

                plog.debug(pkgctx, 'run function ' ||   l_data_list(idx).store_procedure);
                BEGIN
                    EXECUTE IMMEDIATE 'BEGIN :ret := ' || l_data_list(idx).store_procedure || ' ; END;'
                    USING OUT l_Return;

                    IF l_Return = cspkg_errnums.c_success THEN -- chay thanh cong
                        UPDATE cstb_data_later_process SET process_ind = 'Y', process_date = SYSDATE
                        WHERE rowid = l_data_list(idx).rid;
                    ELSE
                        UPDATE cstb_data_later_process SET process_ind = 'F', process_date = SYSDATE,
                                                        process_count  = nvl(process_count, 0) + 1
                        WHERE rowid = l_data_list(idx).rid;
                    END IF;
                EXCEPTION
                    WHEN OTHERS THEN
                        l_error_desc := substr(SQLERRM, 200) || substr(l_data_list(idx).store_procedure, 1500);
                        plog.error(pkgctx, l_error_desc);
                END;
            END LOOP;

            COMMIT;

            EXIT WHEN c_data%NOTFOUND;

        END LOOP;

        CLOSE c_data ;
        plog.setendsection( pkgctx, 'pr_data_later_process' );

    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_error_desc := substr(SQLERRM, 200);
            CLOSE c_data;
            plog.error(pkgctx, l_error_desc);
            plog.setendsection( pkgctx, 'pr_data_later_process' );

    END;


BEGIN
    SELECT   *
    INTO   logrow
    FROM   tlogdebug
    WHERE   ROWNUM <= 1;

    pkgctx      :=
    plog.init('cspkg_data_later_process',
        plevel => logrow.loglevel,
        plogtable => ( logrow.log4table = 'Y' ),
        palert => ( logrow.log4alert = 'Y' ),
        ptrace => ( logrow.log4trace = 'Y' ) );
 END;

/
