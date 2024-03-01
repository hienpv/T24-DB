--------------------------------------------------------
--  DDL for Procedure UPDATEBRANCH
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "UPDATEBRANCH" is

  Cursor vMSGOUT is
    Select * from branch IO;
  --m_IBPSTypeOutIN IBPS_TYPE_CONVERTOUT_OL3;
  --m_VCBTypeOutIN  VCB_TYPE_CONVERTOUT_OL2;
  m_vMSGOUT vMSGOUT%Rowtype;
  --mError          varchar2(2000);
  vremark varchar2(4000);
  iLV_HV  boolean;
BEGIN
  
/*
org_no, 
sb_branch_code, 
bank_no, 
region_code, 
short_name, 
full_name, 
country, 
province 
*/
  OPEN vMSGOUT;
  LOOP
    Fetch vMSGOUT
      into m_vMSGOUT;
    EXIT WHEN vMSGOUT %notfound;
    INSERT INTO bk_bank_org c
      (org_no,
       bank_no,
       region_code,
       description,
       short_name,
       full_name,
       province,
       country,
       sb_branch_code)
    values
      (SEQ_BANK_EXT.NEXTVAL,
       SUBSTR(TRIM(m_vMSGOUT.Branchcode), 3, 3),
       SUBSTR(TRIM(m_vMSGOUT.Branchcode), 1, 2),
       m_vMSGOUT.Branchname,
       CASE WHEN LENGTH(m_vMSGOUT.Branchname) > 50 THEN
       SUBSTR(m_vMSGOUT.Branchname, 0, 50) WHEN
       LENGTH(m_vMSGOUT.Branchname) <= 50 THEN m_vMSGOUT.Branchname END,
       m_vMSGOUT.Branchname,
       SUBSTR(TRIM(m_vMSGOUT.Branchcode), 1, 2), --province code
       'VN',
       m_vMSGOUT.Branchcode
              );
    
    commit;
  end loop;

end updateBranch;

/
