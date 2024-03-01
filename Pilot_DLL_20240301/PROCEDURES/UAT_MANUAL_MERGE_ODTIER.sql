--------------------------------------------------------
--  DDL for Procedure UAT_MANUAL_MERGE_ODTIER
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "UAT_MANUAL_MERGE_ODTIER" 
IS
BEGIN
    delete from si_dat_odtier;
    insert into si_dat_odtier 
    SELECT
      trim(otreci),
      trim(otacct),
      trim(otatyp),
      trim(otseq),
      trim(ottype),
      trim(otrtyp),
      trim(ottpsq),
      trim(otautr),
      trim(otaano),
      trim(otrate),
      trim(otrtn),
      trim(otpvar),
      trim(otpvr1),
      trim(otflor),
      trim(otceil),
      trim(otterm),
      trim(ottrcd),
      trim(otcrtn),
      trim(oturtn),
      trim(otultp),
      trim(otextp),
      trim(otdlmt),
      trim(otalmt),
      trim(otabal),
      trim(otredc),
      trim(otcoll),
      trim(otagd7),
      trim(otrev7),
      trim(otexp7),
      trim(otrdt7),
      trim(otred7),
      trim(otdlm7),
      trim(otdte7),
      trim(otagd6),
      trim(otrev6),
      trim(otexp6),
      trim(otrdt6),
      trim(otred6),
      trim(otdlm6),
      trim(otdte6),
      trim(otpurp),
      trim(OTCLIN),
      trim(otprdt),
      trim(otramt),
      trim(otrpct),
      trim(otrtrm),
      trim(otrtcd),
      trim(otrlmt),
      trim(otres1),
      trim(otres2),
      trim(otres3),
      trim(otres4),
      trim(OTDEPT),
      trim(otaplt),
      trim(otdplt),
      trim(otasts),
        sysdate change_time
    FROM
         SVDATPV51.odtier@dblink_data;
    commit;
   exception
    when others then
         dbms_output.put_line('Error raised: '|| DBMS_UTILITY.FORMAT_ERROR_BACKTRACE || ' - '|| sqlerrm);
         rollback;
END;

/
