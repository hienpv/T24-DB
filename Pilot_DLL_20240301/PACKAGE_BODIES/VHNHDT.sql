--------------------------------------------------------
--  DDL for Package Body VHNHDT
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "VHNHDT" AS
   
PROCEDURE ON_DAYMODE IS 
BEGIN 
       update bk_sys_config set code = 'on'
       where type ='sys_status' and name = 'sys_status';
       commit;
END; 
END VHNHDT;

/
