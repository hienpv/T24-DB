--------------------------------------------------------
--  DDL for Procedure PRC_UPDATE_EBANK_STATUS
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PRC_UPDATE_EBANK_STATUS" 
/* Formatted on 30-Dec-2011 17:20:22 (QP5 v5.126) */
IS
BEGIN

    UPDATE   bb_transfer_history
       SET   status = 'SUCC'
     WHERE   tran_sn IN
                     (SELECT   /*+DRIVING_SITE(cc)*/ bb.tran_sn
                        FROM   (SELECT   tran_sn, status, create_time
                                  FROM   ibs.bb_transfer_history bb
                                 WHERE   TRUNC (bb.create_time) =
                                             TRUNC (SYSDATE)
                                         AND bb.status = 'FAIL') bb,
                               bec.bec_msglog@dblink_tranmap cc
                       WHERE   bb.tran_sn = cc.tran_sn AND cc.resp_code = 0
                               AND SUBSTR (cc.message_date, 0, 8) =
                                      TO_CHAR (SYSDATE, 'yyyyMMdd'));

    COMMIT;

--    UPDATE   bc_transfer_history
--       SET   status = 'SUCC'
--     WHERE   tran_sn IN
--                     (SELECT   /*+DRIVING_SITE(cc)*/ bb.tran_sn
--                        FROM   (SELECT   tran_sn, status, create_time
--                                  FROM   ibs.bc_transfer_history bb
--                                 WHERE   TRUNC (bb.create_time) =
--                                             TRUNC (SYSDATE)
--                                         AND bb.status = 'FAIL') bb,
--                               bec.bec_msglog@dblink_tranmap cc
--                       WHERE   bb.tran_sn = cc.tran_sn AND cc.resp_code = 0
--                               AND SUBSTR (cc.message_date, 0, 8) =
--                                      TO_CHAR (SYSDATE, 'yyyyMMdd'));
--
--    COMMIT;
END;
-- Procedure

/
