--------------------------------------------------------
--  DDL for Procedure REJECTAPPROVERACCBEAUTYIB
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "REJECTAPPROVERACCBEAUTYIB" AS
    BEGIN
      UPDATE BC_ACCOUNT_BEAUTITY_HISTORY 
      SET   STATUS = 'REJECTR',
            DESCRIPTION='SYSTEM REJECTED time:'||TO_CHAR(sysdate,'DD/MM/YYYY HH24:MI:SS')||' do trang thai cap tai khoan qua 1 gio.'
      WHERE status IN('NEWR') ; --('CHO DUYET');
      
      COMMIT;
    END rejectApproverAccBeautyIb;
  END JOB_REMOVE_ACC_BEAUTITY_IB;				 
				

/
