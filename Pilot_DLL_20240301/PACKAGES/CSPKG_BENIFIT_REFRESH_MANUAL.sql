--------------------------------------------------------
--  DDL for Package CSPKG_BENIFIT_REFRESH_MANUAL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKG_BENIFIT_REFRESH_MANUAL" 
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
     **  LocTX      02-12-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_sync_benifit_from_ods(p_date number) ;
    
    PROCEDURE pr_sync_benifit_from_ods_acc(p_date number, p_acc varchar) ;

    PROCEDURE pr_benifit_internal_txn(p_date7 NUMBER);

    PROCEDURE pr_benifit_ibps(p_date7 NUMBER);

    PROCEDURE pr_benifit_swift_in(p_date7 NUMBER);

    PROCEDURE pr_benifit_swift_out(p_date7 NUMBER);

    procedure pr_benifit_vcb_in(p_date7 NUMBER);

    procedure pr_benifit_vcb_out(p_date7 NUMBER);



   -- PROCEDURE pr_benifit_ibps_by_account(p_date7 NUMBER, p_account_no VARCHAR2);

  --  PROCEDURE pr_benifit_internal_by_account(p_date7 NUMBER, p_account_no VARCHAR2);


      PROCEDURE pr_benifit_internal_by_account(p_date7 NUMBER, p_account_no VARCHAR2);--#20161229 Loctx add;

      PROCEDURE pr_benifit_ibps_by_account(p_date7 NUMBER, p_account_no VARCHAR2);--#20161229 LocTX add;

      PROCEDURE pr_benifit_internal_act_ddhist(p_date7 NUMBER, p_account_no VARCHAR2);--#20161229 Loctx add;

      PROCEDURE pr_unload_tw_txn(p_batch_no NUMBER);--#20170214 lOCTX ADD FOR TUNING;
      PROCEDURE pr_load_tw_txn(p_batch_no NUMBER, p_date DATE);--#20170214 lOCTX ADD FOR TUNING

      PROCEDURE pr_benifit_ibps_out_pa2(p_date7 NUMBER, p_account_no VARCHAR2) ;

/*
    Su dung trong truong hop bang rmdel theiu du lieu,
    p_account_no = NULL thi xu ly ALL tai khoan
*/
     PROCEDURE pr_benifit_ibps_in_pa2(p_date7 NUMBER, p_account_no VARCHAR2);

END;

/
