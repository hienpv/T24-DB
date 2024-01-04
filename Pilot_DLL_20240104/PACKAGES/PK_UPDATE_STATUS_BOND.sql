--------------------------------------------------------
--  DDL for Package PK_UPDATE_STATUS_BOND
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."PK_UPDATE_STATUS_BOND" AS

    PROCEDURE updateStatusBond;    
    PROCEDURE revertBondLostData;
END PK_UPDATE_STATUS_BOND;

/
