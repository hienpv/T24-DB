--------------------------------------------------------
--  DDL for Package Body JOB_UPDATE_STAUS_BOND
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."JOB_UPDATE_STAUS_BOND" AS

  PROCEDURE updateStatusBond AS
    BEGIN
    --ton tai ban ghi trong bang BC_DSB_TMTRAN thi update status=succ va ishandle=1 BC_BOND_HISTORY_DETAIL
      FOR data_rec IN (
          SELECT TMTELLID FROM BC_BOND_HISTORY_DETAIL a , BC_DSB_TMTRAN b
          where  (a.SEQ=b.TMTXSEQ or a.SEQ_REVERT=b.TMTXSEQ) and a.TELLER_ID=b.TMTELLID
       )  
    LOOP
          update BC_BOND_HISTORY_DETAIL 
          set 
            status_core= 'SUCC',
            is_handle= '1'       
          where status_core is null;
    END LOOP;
    commit;  
    END updateStatusBond;   

END JOB_UPDATE_STAUS_BOND;

/
