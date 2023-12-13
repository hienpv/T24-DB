--------------------------------------------------------
--  DDL for Package Body CSPKG_CLEAR_TF_SML_TRANSFER
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_CLEAR_TF_SML_TRANSFER" 
  IS
    PROCEDURE pr_delete_tf_sml_transfer
    IS
    BEGIN        
        DELETE FROM PYMT_TF_SML_TRANSFER 
        WHERE CHANGE_TIME < trunc(sysdate) - 2;
        COMMIT;
    EXCEPTION
    WHEN OTHERS
    THEN
        ROLLBACK;        
    END;
END;

/
