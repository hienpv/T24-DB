--------------------------------------------------------
--  DDL for Package WS_GRANTS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."WS_GRANTS" is

rcOK                constant dtype. Counter  %Type := 0;
rcProhibited        constant dtype. Counter  %Type := 2;

rDENIED             constant dtype. Name     %Type := 'N';
rALLOWED            constant dtype. Name     %Type := 'Y';
rLIMITED            constant dtype. Name     %Type := 'L';
rUNDEFINED          constant dtype. Name     %Type := 'U';

rtCONTEXT           constant dtype. Name     %Type := 'context';
rtSUBCONTEXT        constant dtype. Name     %Type := 'subcontext';
rtCLASS_IN_CONTEXT  constant dtype. Name     %Type := 'data_class_in_context';
rtACTION_IN_CONTEXT constant dtype. Name     %Type := 'action_in_context';
rtLAYOUT            constant dtype. Name     %Type := 'layout';

rtDATACLASS         constant dtype. Name     %Type := 'data_class';
rtATTRIBUTE         constant dtype. Name     %Type := 'attribute';
rtACTION            constant dtype. Name     %Type := 'action';
rtQUERY             constant dtype. Name     %Type := 'query';
rtACTION_PARM       constant dtype. Name     %Type := 'action_parm';
rtBATCH             constant dtype. Name     %Type := 'batch';
rtBATCH_STEP        constant dtype. Name     %Type := 'batch_step';
rtBATCH_PARM        constant dtype. Name     %Type := 'batch_parm';
rtBATCH_IN_FOLDWER  constant dtype. Name     %Type := 'batch_in_folder';
rtPAGE              constant dtype. Name     %Type := 'page';
rtPROCESS           constant dtype. Name     %Type := 'process';

rtLISTBOX           constant dtype. Name     %Type := 'listbox';
rtLISTBOX_ITEM      constant dtype. Name     %Type := 'listbox_item';

tvDOMAIN      constant dtype. Name %Type := 'DOMAIN';
tvDOMAIN_TYPE constant dtype. Name %Type := 'DOMAIN_TYPE';
tvDATA_TYPE   constant dtype. Name %Type := 'TYPE';
tvTAG_TYPE    constant dtype. Name %Type := 'TAG_TYPE';
tvTAG_NAME    constant dtype. Name %Type := 'TAG_NAME';
tvDDDW_SELECT constant dtype. Name %Type := 'DDDW_SELECT';
tvDDDW_FILTER constant dtype. Name %Type := 'DDDW_FILTER';
tvACTION_TYPE constant dtype. Name %Type := 'TYPE';
tvDATA_LENGTH constant dtype. Name %Type := 'LENGTH';

tvNUMBER      constant dtype. Name %Type := 'NUMB';
tvTYPE        constant dtype. Name %Type := 'TYPE';
tvVALUE       constant dtype. Name %Type := 'VALUE';
tvINIT_VALUE  constant dtype. Name %Type := 'INIT_VALUE';
tvCAN_EDIT    constant dtype. Name %Type := 'EDIT';

-- data/domain type
dtSTRING      constant dtype. Name %Type := 'string';
dtDATE        constant dtype. Name %Type := 'datetime';
dtDECIMAL     constant dtype. Name %Type := 'decimal';
dtINT         constant dtype. Name %Type := 'integer';
dtID          constant dtype. Name %Type := 'id';

dtEDIT        constant dtype. Name %Type := 'EDIT';
dtDDDW        constant dtype. Name %Type := 'DDDW';
dtLISTBOX     constant dtype. Name %Type := 'LISTBOX';

opSTART_SESSION     constant dtype. Name     %Type := 'SysStartSessionExt';
function CHECK_OBJECT(
  DatasetCode    in dtype. Name     %Type,
  ObjectID       in dtype. RecordID %Type
) return dtype. Counter %Type;

end ws_grants;

/
