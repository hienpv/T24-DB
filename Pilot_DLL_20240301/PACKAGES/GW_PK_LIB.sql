--------------------------------------------------------
--  DDL for Package GW_PK_LIB
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "GW_PK_LIB" is
   -- Public type declarations
  TYPE m_tblField_type IS TABLE OF Varchar2(2000) INDEX BY Varchar2(20);


    FUNCTION SWIFT_RM_GETFIELD_IN(pSWiftFile   varchar2,
                                  pclobContent clob,
                                  m_MSG_TYPE   varchar2) return m_tblField_type;


   FUNCTION GET_SWIFT_Field(pCOntent   clob,
                             pFiledCode varchar2,
                             pRownum    number,
                             pPartnum   number,
                             m_MSG_TYPE varchar2) Return Varchar2;

    FUNCTION GetFieldValue(pSWFieldTag  varchar2,
                           piRowNum     integer,
                           piPartNum    Integer,
                           pFilecontent m_tblField_type) return Varchar2;

    FUNCTION SWIFT_RM_GETFIELD_IN(pSWiftFile   varchar2,
                                  pclobContent clob,
                                  m_MSG_TYPE   varchar2,
                                  prownum      integer) return m_tblField_type;

    FUNCTION SWIFT_RM_GETFIELD_IN(pFieldName varchar2, pCONTENT clob)
      return varchar2;

    FUNCTION GET_IBPS_Field(pCOntent varchar2, FiledCode varchar2)
      Return Varchar2;

end GW_PK_LIB;

/
