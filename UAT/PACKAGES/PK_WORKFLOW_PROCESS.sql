--------------------------------------------------------
--  DDL for Package PK_WORKFLOW_PROCESS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PK_WORKFLOW_PROCESS" AS 

  PROCEDURE PROCESS_CONFIG_WORKFLOW(P_CIF_NO in varchar2, P_PROCESS_DEFINITION_KEY in varchar2);
  
  PROCEDURE PROCESS_CONFIG_WORKFLOW_2_LEV (
    P_CIF_NO in varchar2, 
    P_PROCESS_DEFINITION_KEY in varchar2,
    P_ASSIGNEE_LEVEL1 in varchar2,
    P_ASSIGNEE_LEVEL2 in varchar2
  );

END PK_WORKFLOW_PROCESS;

/
