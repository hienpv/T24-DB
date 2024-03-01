--------------------------------------------------------
--  DDL for Package CSPKG_PARAM_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKG_PARAM_SYNC" 
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

      PROCEDURE pr_fee_discount_sync(dm_operation_type in CHAR,
                                      orchestrate_cfcifn in NUMBER,
                                      orchestrate_cfsnme in VARCHAR,
                                      orchestrate_cfbust in VARCHAR,
                                      orchestrate_cfoffr in VARCHAR);
/*

      PROCEDURE pr_acct_product_dd_sync_remove(dm_operation_type in CHAR,
                                      orchestrate_sccode in VARCHAR2,
                                      orchestrate_dp2cur in VARCHAR2,
                                      orchestrate_pscdes in VARCHAR2);

        PROCEDURE pr_acct_product_ln_sync_remove(dm_operation_type in CHAR,
                                  orchestrate_ptype in VARCHAR2,
                                  orchestrate_ptydsc in VARCHAR2,
                                  orchestrate_plngrp in VARCHAR2,
                                  orchestrate_pgrdsc in VARCHAR2,
                                  orchestrate_pcurty in VARCHAR2);

        PROCEDURE pr_bussiness_type_sync (dm_operation_type in CHAR,
                                  orchestrate_vcdesc in VARCHAR2,
                                  orchestrate_vcesec in VARCHAR2) ;

        PROCEDURE pr_currucy_sync_remove (dm_operation_type in CHAR,
                                  orchestrate_jfxcod in VARCHAR,
                                  orchestrate_jfxdsc in VARCHAR);



        PROCEDURE pr_id_type_sync(dm_operation_type in CHAR,
                              orchestrate_cfidcd in VARCHAR,
                              orchestrate_cfidsc in VARCHAR,
                              orchestrate_cfidct in VARCHAR);

        PROCEDURE pr_officer_sync( dm_operation_type in CHAR,
                              orchestrate_ssooff in VARCHAR,
                              orchestrate_ssobrn in NUMBER,
                              orchestrate_ssoidn in VARCHAR,
                              orchestrate_ssonam in VARCHAR,
                              orchestrate_ssosna in VARCHAR,
                              orchestrate_ssocur in VARCHAR,
                              orchestrate_ssoalg in NUMBER,
                              orchestrate_ssoalm in NUMBER,
                              orchestrate_sscntr in NUMBER,
                              orchestrate_ssdept in NUMBER) ;

        PROCEDURE pr_region_sync(dm_operation_type in CHAR,
                              orchestrate_prov_code in VARCHAR,
                              orchestrate_fullname in VARCHAR2);
*/
      PROCEDURE pr_cfoffl_sync(dm_operation_type in CHAR,
                                      orchestrate_cfaccn in NUMBER,
                                      orchestrate_cfatyp in CHAR,
                                      orchestrate_cfoffr in VARCHAR);

END; -- Package spec

/
