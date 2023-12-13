--------------------------------------------------------
--  DDL for Package Body PK_UPDATE_STATUS_BOND
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."PK_UPDATE_STATUS_BOND" AS

  PROCEDURE updateStatusBond AS
  countBond NUMBER;
    BEGIN
--    --ton tai ban ghi trong bang BC_DSB_TMTRAN thi update status=succ va ishandle=1 BC_BOND_HISTORY_DETAIL
--    Select count(1)  into countBond from CSTB_ETL_LOG where 
--    PROCESS_NAME='SEQ_EBANK_TMTRAN' and STATUS='O' and PARAM_DATA=TO_CHAR(sysdate,'ddMMyy');  --A Manh
--    DBMS_OUTPUT.put_line('countBond = ' || countBond);
--     IF countBond = 0 THEN        
--      FOR data_rec IN (
--          SELECT TMTELLID FROM BC_BOND_HISTORY_DETAIL a , BC_DSB_TMTRAN b
--          where  (a.SEQ=b.TMTXSEQ or a.SEQ_REVERT=b.TMTXSEQ) and a.TELLER_ID=b.TMTELLID
--           and  TRUNC(a.create_time) >= TRUNC(SYSDATE -3) and TRUNC(a.create_time) <= TRUNC(SYSDATE) 
--       )  
--     LOOP
--          update BC_BOND_HISTORY_DETAIL 
--          set 
--            STATUS_REVERT= 'SUCC',
--            is_handle= '1'       
--          where STATUS_REVERT is NOT null OR STATUS_REVERT <> '0';
--    END LOOP;
--    
--    ELSE 
--     update BC_BOND_HISTORY_DETAIL 
--          set 
--            STATUS_REVERT= 'SUCC',
--            is_handle= '1'       
--          where STATUS_REVERT is NOT null OR STATUS_REVERT <> '0';
--    END IF;
--     DBMS_OUTPUT.PUT_LINE(countBond);
--    
--    update BK_SYS_PARAMETER set name= 'ON' where  type='BOND'; --update flag bond 
--    commit;  
    null;
    END updateStatusBond;   
    
    PROCEDURE revertBondLostData as
        checkExist number;
        CURSOR bond is
        Select * from bk_account_history 
        where trim(TELLER_ID) = 'IBDSBUSR'
            and TRACE_CODE like 'BOND%' 
            and DC_SIGN ='D' 
            and status='SUCC' 
            and  TRAN_TIME > sysdate-1
        order by tran_time desc;
        service_item bond%ROWTYPE;
    BEGIN
--        OPEN bond;
--            LOOP
--            FETCH bond INTO service_item;
--            EXIT WHEN bond%NOTFOUND;
--            BEGIN              
--                IF service_item.TM_SEQ IS NOT NULL
--                THEN 
--                    select count(1) into checkExist from BC_BOND_HISTORY_DETAIL 
--                    where SEQ=service_item.TM_SEQ;
--                    if(checkExist >0) then
--                        UPDATE BC_BOND_HISTORY_DETAIL set STATUS_REVERT= 'SUCC', is_handle= '1' where SEQ=service_item.TM_SEQ;
--                        commit;
--                    
--                    end if;
--                    
--                END IF;
--           
--            EXCEPTION
--                WHEN OTHERS THEN
--                    ROLLBACK;
--                    dbms_output.PUT_LINE('revertBondLostData error - '||SQLCODE||' -ERROR- '||SQLERRM );    
--            END;
--        END LOOP;
--        update BK_SYS_PARAMETER set name= 'ON' where  type='BOND'; --update flag bond 
--        commit;
--        CLOSE bond; 
    null;
    END revertBondLostData;  

END PK_UPDATE_STATUS_BOND;

/
