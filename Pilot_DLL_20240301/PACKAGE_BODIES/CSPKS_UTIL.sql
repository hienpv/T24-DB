--------------------------------------------------------
--  DDL for Package Body CSPKS_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKS_UTIL" 
/* Formatted on 29-Aug-2012 11:00:28 (QP5 v5.126) */
IS
    --
    -- To modify this template, edit file PKGBODY.TXT in TEMPLATE
    -- directory of SQL Navigator
    --
    -- Purpose: Briefly explain the functionality of the package body
    --
    -- MODIFICATION HISTORY
    -- Person      Date    Comments
    -- ---------   ------  ------------------------------------------
    -- Enter procedure, function bodies as shown below
    pkgctx   plog.log_ctx;
    logrow   tlogdebug%ROWTYPE;


    FUNCTION fn_get_seq (p_name              IN     VARCHAR2,
                         p_key               IN     VARCHAR2,
                         p_seq                  OUT VARCHAR2,
                         p_auto_num_length   IN     NUMBER,
                         p_err_code             OUT VARCHAR2)
        RETURN NUMBER
    IS
        l_last_num   VARCHAR2 (30);
    BEGIN
        plog.setbeginsection (pkgctx, 'fn_get_seq');

        BEGIN
            INSERT INTO cstb_seq
               VALUES   (p_name, p_key, 1)
            RETURNING   last_num
                 INTO   l_last_num;
        EXCEPTION
            WHEN DUP_VAL_ON_INDEX
            THEN
                   UPDATE   cstb_seq
                      SET   last_num = last_num + 1
                    WHERE   seq_name = p_name AND seq_key = p_key
                RETURNING   last_num
                     INTO   l_last_num;
        END;

        p_seq := p_key || LPAD (l_last_num, p_auto_num_length, '0');
        --plog.debug (pkgctx, 'p_seq ' || p_seq);
        COMMIT;
        plog.setendsection (pkgctx, 'fn_get_seq');
        RETURN 1;
    EXCEPTION
        WHEN OTHERS
        THEN
            ROLLBACK;
            plog.error (pkgctx, SQLERRM);
            p_err_code := 'CS-001';
            plog.setendsection (pkgctx, 'fn_get_seq');
            RETURN -1;
    END fn_get_seq;


BEGIN
    SELECT   *
      INTO   logrow
      FROM   tlogdebug
     WHERE   ROWNUM <= 1;

    pkgctx :=
        plog.init ('cspks_util',
                   plevel      => logrow.loglevel,
                   plogtable   => (logrow.log4table = 'Y'),
                   palert      => (logrow.log4alert = 'Y'),
                   ptrace      => (logrow.log4trace = 'Y'));
END;

/
