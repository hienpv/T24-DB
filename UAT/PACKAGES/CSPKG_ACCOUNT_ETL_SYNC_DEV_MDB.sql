--------------------------------------------------------
--  DDL for Package CSPKG_ACCOUNT_ETL_SYNC_DEV_MDB
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."CSPKG_ACCOUNT_ETL_SYNC_DEV_MDB" 
IS
/*----------------------------------------------------------------------------------------------------
 ** Module   : COMMODITY SYSTEM
 ** and is copyrighted by FSS.
 **
 **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
 **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
 **    graphic, optic recording or otherwise, translated in any language or computer language,
 **    without the prior written permission of Financial Software Solutions. JSC.
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  LocTX      31/10/2014    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/

    TYPE ty_varchar2_tb IS TABLE OF VARCHAR2(30) INDEX BY BINARY_INTEGER;
    g_limit_count CONSTANT NUMBER := 10000;


    PROCEDURE pr_lnmast_sync;
    PROCEDURE pr_lnmemo_cdc_override ;
    PROCEDURE pr_ddmast_sync ;
    PROCEDURE pr_ddmemo_cdc_override;
    PROCEDURE pr_cdmast_sync ;
    PROCEDURE pr_cdmemo_cdc_override;
    PROCEDURE pr_passbook_no_sync ;

 END;

/
