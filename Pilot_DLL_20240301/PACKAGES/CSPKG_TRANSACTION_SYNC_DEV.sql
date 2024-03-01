--------------------------------------------------------
--  DDL for Package CSPKG_TRANSACTION_SYNC_DEV
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKG_TRANSACTION_SYNC_DEV" 
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



    PROCEDURE pr_salary_onday_sync( dm_operation_type in CHAR,
                                    orchestrate_trdate in NUMBER,
                                    orchestrate_treffd in NUMBER,
                                    orchestrate_trtime in NUMBER,
                                    orchestrate_seq in NUMBER,
                                    orchestrate_truser in VARCHAR,
                                    orchestrate_dorc in VARCHAR,
                                    orchestrate_amt in NUMBER,
                                    orchestrate_trctyp in VARCHAR,
                                    orchestrate_trefth in VARCHAR2,
                                    orchestrate_tracct in NUMBER,
                                    orchestrate_trancd in NUMBER
                                    );


      PROCEDURE  pr_tmtran_sync (dm_operation_type in CHAR,
                                  orchestrate_tmtxcd in VARCHAR,
                                  orchestrate_tmresv07 in VARCHAR,
                                  orchestrate_tmdorc in CHAR,
                                  orchestrate_tmtxamt in NUMBER,
                                  orchestrate_tmglcur in VARCHAR,
                                  orchestrate_tmorgamt in NUMBER,
                                  orchestrate_tmefth in VARCHAR2,
                                  orchestrate_tmacctno in NUMBER,
                                  orchestrate_tmtellid in VARCHAR,
                                  orchestrate_tmtxseq in NUMBER,
                                  orchestrate_tmtxstat in VARCHAR,
                                  orchestrate_tmhosttxcd in NUMBER,
                                  orchestrate_tmapptype in CHAR,
                                  orchestrate_tmeqvtrn in CHAR,
                                  orchestrate_tmibttrn in CHAR,
                                  orchestrate_tmsumtrn in CHAR,
                                  orchestrate_tmentdt7 in NUMBER,
                                  orchestrate_tmeffdt7 in NUMBER,
                                  orchestrate_tmsseq in NUMBER,
                                  orchestrate_tmtiment in NUMBER);



      PROCEDURE  pr_tmtran_fail_sync (dm_operation_type in CHAR,
                                  orchestrate_tmtxcd in VARCHAR,
                                  orchestrate_tmresv07 in VARCHAR,
                                  orchestrate_tmdorc in CHAR,
                                  orchestrate_tmtxamt in NUMBER,
                                  orchestrate_tmglcur in VARCHAR,
                                  orchestrate_tmorgamt in NUMBER,
                                  orchestrate_tmefth in VARCHAR2,
                                  orchestrate_tmacctno in NUMBER,
                                  orchestrate_tmtellid in VARCHAR,
                                  orchestrate_tmtxseq in NUMBER,
                                  orchestrate_tmtxstat in VARCHAR,
                                  orchestrate_tmhosttxcd in NUMBER,
                                  orchestrate_tmapptype in CHAR,
                                  orchestrate_tmeqvtrn in CHAR,
                                  orchestrate_tmibttrn in CHAR,
                                  orchestrate_tmsumtrn in CHAR,
                                  orchestrate_tmentdt7 in NUMBER,
                                  orchestrate_tmeffdt7 in NUMBER,
                                  orchestrate_tmsseq in NUMBER,
                                  orchestrate_tmtiment in NUMBER);


   PROCEDURE  pr_tmtran_fail_later_process;



END; -- Package spec

/
