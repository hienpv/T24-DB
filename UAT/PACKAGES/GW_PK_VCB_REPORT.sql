--------------------------------------------------------
--  DDL for Package GW_PK_VCB_REPORT
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."GW_PK_VCB_REPORT" IS
  TYPE m_tblField_type IS TABLE OF Varchar2(2000) INDEX BY Varchar2(20);


  FUNCTION SWIFT_RM_GETFIELD_IN(pSWiftFile   varchar2,
                                pclobContent clob,
                                m_MSG_TYPE   varchar2) return m_tblField_type;
  FUNCTION SWIFT_RM_GETFIELD_IN(pSWiftFile   varchar2,
                                pclobContent clob,
                                m_MSG_TYPE   varchar2,
                                prownum      integer) return m_tblField_type;
  FUNCTION SWIFT_RM_GETFIELD_IN(pFieldName varchar2, pCONTENT clob)
    return varchar2;
  /*  FUNCTION SWIFT_RM_GETFIELD_IN(pSWiftFile   varchar2,
  pclobContent clob,
  pFile_name   varchar2) return m_tblField_type;*/
  FUNCTION VCB_GET_FIELD(pCONTENT   VARCHAR2,
                         pFieldName VARCHAR2,
                         m_MSG_TYPE nvarchar2) RETURN VARCHAR2;
  FUNCTION VCB_GET_SWIFT_Field(pCOntent   clob,
                               pFiledCode varchar2,
                               pRownum    number,
                               pPartnum   number,
                               m_MSG_TYPE varchar2) Return Varchar2;
  FUNCTION GetFieldValue(pSWFieldTag  varchar2,
                         piRowNum     integer,
                         piPartNum    Integer,
                         pFilecontent m_tblField_type) return Varchar2;

END; -- Package spec

/
