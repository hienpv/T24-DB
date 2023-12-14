--------------------------------------------------------
--  DDL for Procedure PROC_CIFCORP_SYNC
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "PROC_CIFCORP_SYNC" is
begin

  Delete from bb_corp_class;
  INSERT INTO bb_corp_class
    (cif_no,
     class,
     cfsic1,
     cfsic2,
     cfsic3,
     cfsic4,
     cfsic5,
     cfsic6,
     cfsic7,
     cfsic8)
    SELECT cfcifn,
           FN_GET_CLASS_CIF(CFINDI,
                            cfsic1,
                            cfsic2,
                            cfsic3,
                            cfsic4,
                            cfsic5,
                            cfsic6,
                            cfsic7,
                            cfsic8) as class,
           cfsic1,
           cfsic2,
           cfsic3,
           cfsic4,
           cfsic5,
           cfsic6,
           cfsic7,
           cfsic8
      from svdatpv51.cfmast@dblink_data1
     where cfcifn in (select cif_no from bb_corp_info);
  commit;
end proc_cifCorp_sync;

/
