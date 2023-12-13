--------------------------------------------------------
--  DDL for Package CSPKG_CIF_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKG_CIF_SYNC" 
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


   PROCEDURE pr_all_address_cif_sync( dm_operation_type IN CHAR,
                                      orchestrate_cfaccn IN NUMBER,
                                      orchestrate_cfeadd IN VARCHAR2,
                                      orchestrate_cfeadc IN VARCHAR2,
                                      orchestrate_cfatyp IN CHAR);

    PROCEDURE pr_cfmast_sync ( dm_operation_type in CHAR,
                                  orchestrate_cfcifn in NUMBER,
                                  orchestrate_cfsscd in VARCHAR2,
                                  orchestrate_cfssno in VARCHAR2,
                                  orchestrate_cfbrnn in VARCHAR2, -- 062023 QUANDH3: T24 change data type  -- orchestrate_cfbrnn in NUMBER, 
                                  orchestrate_cfna1 in VARCHAR2,
                                  orchestrate_cfbird in NUMBER,
                                  orchestrate_cfbirp in VARCHAR2,
                                  orchestrate_cfcitz in VARCHAR2,
                                  orchestrate_cfindi in VARCHAR2,
                                  orchestrate_taxcod in VARCHAR2);

    PROCEDURE pr_cfaddr_sync ( dm_operation_type in CHAR,
                              orchestrate_cfcifn in NUMBER,
                              orchestrate_cfadsq in VARCHAR2,
                              orchestrate_cfna2 in VARCHAR2 );

      PROCEDURE pr_cftnam_sync(dm_operation_type IN CHAR,
                              orchestrate_cttcif IN  NUMBER ,
                              orchestrate_ctname IN VARCHAR,
                              orchestrate_ctadd1 IN VARCHAR,
                              orchestrate_ctadd2 IN VARCHAR,
                              orchestrate_ctssno IN VARCHAR,
                              orchestrate_ctsscd IN VARCHAR,
                              orchestrate_ctsnme IN VARCHAR,
                              orchestrate_cthpho IN VARCHAR,
                              orchestrate_ctbpho IN VARCHAR,
                              orchestrate_ctbfho IN VARCHAR,
                              orchestrate_ctcoun IN VARCHAR,
                              orchestrate_ctpur6 IN NUMBER ,
                              orchestrate_ctpur7 IN NUMBER ,
                              orchestrate_ctidi6 IN NUMBER ,
                              orchestrate_ctidi7 IN NUMBER ,
                              orchestrate_ctista IN VARCHAR,
                              orchestrate_ctiplc IN VARCHAR,
                              orchestrate_ctirm1 IN VARCHAR,
                              orchestrate_ctirm2 IN VARCHAR,
                              orchestrate_ctirm3 IN VARCHAR);

END; -- Package spec

/
