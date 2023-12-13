--------------------------------------------------------
--  DDL for Package CSPKG_ACCOUNT_SYNC_DEV_MDB
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."CSPKG_ACCOUNT_SYNC_DEV_MDB" 
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

    c_bank_no CONSTANT VARCHAR2(10) := '302';

      PROCEDURE pr_cdmemo_sync (dm_operation_type in CHAR,
                              orchestrate_acctno in NUMBER,
                              orchestrate_curtyp in VARCHAR,
                              orchestrate_cbal in NUMBER,
                              orchestrate_accint in NUMBER,
                              orchestrate_penamt in NUMBER,
                              orchestrate_hold in NUMBER,
                              orchestrate_wdrwh in NUMBER,
                              orchestrate_cdnum in NUMBER,
                              orchestrate_status in NUMBER);

      PROCEDURE pr_cdtnew_sync( dm_operation_type in CHAR,
                          orchestrate_bankno in NUMBER,
                          orchestrate_brn in NUMBER,
                          orchestrate_curtyp in VARCHAR,
                          orchestrate_cifno in NUMBER,
                          orchestrate_orgbal in NUMBER,
                          orchestrate_cbal in NUMBER,
                          orchestrate_accint in NUMBER,
                          orchestrate_penamt in NUMBER,
                          orchestrate_hold in NUMBER,
                          orchestrate_wdrwh in NUMBER,
                          orchestrate_cdnum in NUMBER,
                          orchestrate_issdt in NUMBER,
                          orchestrate_matdt in NUMBER,
                          orchestrate_rnwctr in NUMBER,
                          orchestrate_status in NUMBER,
                          orchestrate_acname in VARCHAR,
                          orchestrate_acctno in NUMBER,
                          orchestrate_type in VARCHAR,
                          orchestrate_rate in NUMBER,
                          orchestrate_renew in VARCHAR,
                          orchestrate_dactn in NUMBER,
                          orchestrate_cdterm in NUMBER,
                          orchestrate_cdmuid in VARCHAR);


      PROCEDURE pr_ddtnew_sync (dm_operation_type IN CHAR,
                                orchestrate_branch  IN NUMBER,
                                orchestrate_acctno IN NUMBER,
                                orchestrate_actype IN VARCHAR,
                                orchestrate_ddctyp IN VARCHAR,
                                orchestrate_cifno  IN NUMBER,
                                orchestrate_status  IN NUMBER,
                                orchestrate_hold IN NUMBER,
                                orchestrate_cbal IN NUMBER,
                                orchestrate_odlimt IN NUMBER,
                                orchestrate_rate  IN NUMBER,
                                orchestrate_acname IN VARCHAR,
                                orchestrate_sccode IN VARCHAR,
                                orchestrate_datop7 IN NUMBER,
                                orchestrate_accrue IN NUMBER);



     PROCEDURE pr_lntnew_sync (dm_operation_type in CHAR,
                              orchestrate_brn in NUMBER,
                              orchestrate_accint in NUMBER,
                              orchestrate_cifno in NUMBER,
                              orchestrate_lnnum in NUMBER,
                              orchestrate_acctno in NUMBER,
                              orchestrate_purcod in VARCHAR,
                              orchestrate_curtyp in VARCHAR,
                              orchestrate_orgamt in NUMBER,
                              orchestrate_cbal in NUMBER,
                              orchestrate_ysobal in NUMBER,
                              orchestrate_billco in NUMBER,
                              orchestrate_freq in NUMBER,
                              orchestrate_ipfreq in NUMBER,
                              orchestrate_fulldt in NUMBER,
                              orchestrate_status in NUMBER,
                              orchestrate_odind in VARCHAR,
                              orchestrate_bilesc in NUMBER,
                              orchestrate_biloc in NUMBER,
                              orchestrate_bilmc in NUMBER,
                              orchestrate_bilprn in NUMBER,
                              orchestrate_bilint in NUMBER,
                              orchestrate_billc in NUMBER,
                              orchestrate_pmtamt in NUMBER,
                              orchestrate_fnlpmt in NUMBER,
                              orchestrate_drlimt in NUMBER,
                              orchestrate_hold in NUMBER,
                              orchestrate_accmlc in VARCHAR,
                              orchestrate_comacc in NUMBER,
                              orchestrate_othchg in NUMBER,
                              orchestrate_acname in VARCHAR,
                              orchestrate_type in VARCHAR,
                              orchestrate_datopn in NUMBER,
                              orchestrate_matdt in NUMBER,
                              orchestrate_freldt in VARCHAR,
                              orchestrate_rate in VARCHAR,
                              orchestrate_term in NUMBER,
                              orchestrate_tmcode in VARCHAR,
                            orchestrate_before_acctno in NUMBER,
                            orchestrate_before_cifno in NUMBER);

      PROCEDURE pr_ddmemo_sync (dm_operation_type in CHAR,
                              orchestrate_acctno IN NUMBER,
                                orchestrate_status IN NUMBER,
                                orchestrate_hold IN NUMBER,
                                orchestrate_cbal IN NUMBER,
                                orchestrate_odlimt IN NUMBER,
                                orchestrate_acname IN VARCHAR2,
                                orchestrate_dla7 IN NUMBER
                                );


       PROCEDURE pr_cdgroup_sync(dm_operation_type IN CHAR,
                                  orchestrate_cfgnam IN VARCHAR,
                                  orchestrate_cfgcur IN VARCHAR,
                                  orchestrate_cfagd7 IN NUMBER,
                                  orchestrate_cfgsts IN CHAR,
                                  orchestrate_cfagno IN NUMBER,
                                  orchestrate_cfcifn IN NUMBER);


      PROCEDURE pr_lnmemo_sync (dm_operation_type in CHAR,
                              orchestrate_accint IN NUMBER,
                              orchestrate_curtyp IN VARCHAR,
                              orchestrate_cbal IN NUMBER,
                              orchestrate_bilprn IN NUMBER ,
                              orchestrate_bilint IN NUMBER,
                              orchestrate_billc IN NUMBER,
                              orchestrate_bilesc IN NUMBER ,
                              orchestrate_biloc IN NUMBER,
                              orchestrate_bilmc IN NUMBER,
                              orchestrate_drlimt IN NUMBER,
                              orchestrate_hold IN NUMBER,
                              orchestrate_comacc IN NUMBER,
                              orchestrate_othchg IN NUMBER,
                              orchestrate_acctno IN NUMBER);





      PROCEDURE pr_ddmast_sync (dm_operation_type in CHAR,
                              orchestrate_before_status IN NUMBER,
                              orchestrate_before_acname  IN VARCHAR2,
                              orchestrate_before_cifno IN NUMBER,
                              orchestrate_before_branch IN NUMBER,
                              orchestrate_acctno IN NUMBER,
                              orchestrate_status IN NUMBER,
                              orchestrate_acname IN VARCHAR2,
                              orchestrate_cifno IN NUMBER,
                              orchestrate_branch IN NUMBER
                              );


      PROCEDURE pr_cdmast_sync (dm_operation_type in CHAR,
                              orchestrate_before_status in NUMBER,
                              orchestrate_before_acname IN VARCHAR2,
                              --orchestrate_before_type IN VARCHAR2,
                              --orchestrate_before_brn IN NUMBER,
                              --orchestrate_before_cifno IN NUMBER,
                              --orchestrate_before_hold IN NUMBER,
                              --orchestrate_before_cdnum IN NUMBER,
                              --orchestrate_before_rate IN NUMBER,

                              orchestrate_acctno IN NUMBER,
                              orchestrate_status IN NUMBER,
                              orchestrate_acname IN VARCHAR2,
                              orchestrate_cdtcod IN CHAR--,
                              --orchestrate_type IN VARCHAR2,
                              --orchestrate_brn IN NUMBER,
                              --orchestrate_cifno IN NUMBER,
                              --orchestrate_hold IN NUMBER,
                              --orchestrate_cdnum IN NUMBER,
                              --orchestrate_rate IN NUMBER
                              );

      PROCEDURE pr_lnmast_sync (dm_operation_type in CHAR,
                              orchestrate_before_status in NUMBER,
                              --orchestrate_before_acname IN VARCHAR2,
                              orchestrate_before_type IN VARCHAR2,
                              --orchestrate_before_brn IN NUMBER,
                              --orchestrate_before_cifno IN NUMBER,
                              orchestrate_before_orgamt IN NUMBER,
                              orchestrate_before_term IN NUMBER,
                              orchestrate_before_tmcode IN VARCHAR2,
                              orchestrate_before_pmtamt IN NUMBER,
                              orchestrate_before_fnlpmt IN NUMBER,
                              --orchestrate_before_offcr VARCHAR2,
                              orchestrate_before_rate IN NUMBER,
                              orchestrate_acctno IN NUMBER,
                              orchestrate_status IN NUMBER,
                              --orchestrate_acname IN VARCHAR2,
                              orchestrate_type IN VARCHAR2,
                              --orchestrate_brn IN NUMBER,
                              --orchestrate_cifno IN NUMBER,
                              orchestrate_orgamt IN NUMBER,
                              orchestrate_term IN NUMBER,
                              orchestrate_tmcode IN VARCHAR2,
                              orchestrate_pmtamt IN NUMBER,
                              orchestrate_fnlpmt IN NUMBER,
                              --orchestrate_offcr VARCHAR2,
                              orchestrate_rate IN NUMBER
                              );

END; -- Package spec

/
