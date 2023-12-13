--------------------------------------------------------
--  DDL for Package Body JOB_REMOVE_ACC_BEAUTITY_IB
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."JOB_REMOVE_ACC_BEAUTITY_IB" AS

  PROCEDURE rejectApproverAccBeautyIb AS
    BEGIN
      UPDATE BC_ACCOUNT_BEAUTITY_HISTORY 
      SET   STATUS = 'REJECTR',
            DESCRIPTION='SYSTEM REJECTED time:'||TO_CHAR(sysdate,'DD/MM/YYYY HH24:MI:SS')||' do trang thai cap tai khoan qua 10p.'
      WHERE status IN('NEWR') ; --('CHO DUYET');
     -- DELETE BC_CREDIT_CARD_HISTORY;
      COMMIT;
    END rejectApproverAccBeautyIb;
  END JOB_REMOVE_ACC_BEAUTITY_IB;

/
