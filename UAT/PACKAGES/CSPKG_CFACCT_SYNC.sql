--------------------------------------------------------
--  DDL for Package CSPKG_CFACCT_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."CSPKG_CFACCT_SYNC" 
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
 **  HaoNS      09-SEP-2014    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/



      PROCEDURE pr_cfacct_sync (dm_operation_type in CHAR,
                              orchestrate_CFACID in varchar, 
                                orchestrate_CFCIFN in NUMBER, 
                                orchestrate_CFCIFT in varchar, 
                                orchestrate_CFUATY in varchar, 
                                orchestrate_CFACCN in NUMBER, 
                                orchestrate_CFATYP in varchar, 
                                orchestrate_CFAREF in varchar, 
                                orchestrate_CFSNME in varchar, 
                                orchestrate_CFFSNM in varchar,  
                                orchestrate_CFRELA in varchar,   
                                orchestrate_CFRELP in varchar,   
                                orchestrate_CFAALA in NUMBER,  
                                orchestrate_CFAALE in NUMBER,  
                                orchestrate_CFALIS in NUMBER,  
                                orchestrate_CFACJP in varchar, 
                                orchestrate_CFACJN in varchar, 
                                orchestrate_CFNOTC in varchar, 
                                orchestrate_CFANOS in NUMBER,   
                                orchestrate_CFHLDM in varchar, 
                                orchestrate_CFICSM in varchar, 
                                orchestrate_CFAPAO in NUMBER, 
                                orchestrate_CFAPIO in NUMBER, 
                                orchestrate_CFGTED in NUMBER, 
                                orchestrate_CFGTEP in NUMBER, 
                                orchestrate_CFSHRP in NUMBER, 
                                orchestrate_CFADLM in NUMBER, 
                                orchestrate_CFADL6 in NUMBER, 
                                orchestrate_CFMTIM in NUMBER, 
                                orchestrate_CFUCD7 in NUMBER, 
                                orchestrate_CFUCD6 in NUMBER );
END;

/
