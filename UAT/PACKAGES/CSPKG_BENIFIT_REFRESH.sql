--------------------------------------------------------
--  DDL for Package CSPKG_BENIFIT_REFRESH
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."CSPKG_BENIFIT_REFRESH" 
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



    PROCEDURE pr_benifit_internal_txn(p_date7 NUMBER);

    PROCEDURE pr_benifit_internal_txn_on_day;

    PROCEDURE pr_benifit_ibps(p_date7 NUMBER);

    PROCEDURE pr_benifit_swift_in(p_date7 NUMBER);

    PROCEDURE pr_benifit_swift_out(p_date7 NUMBER);

    PROCEDURE pr_benifit_msg_on_day;


    procedure pr_benifit_vcb_in(p_date7 NUMBER);

    procedure pr_benifit_vcb_out(p_date7 NUMBER);

    PROCEDURE pr_benifit_rmsid_th2(p_date7 NUMBER);

    PROCEDURE pr_benifit_rmsid_th1(p_date7 NUMBER);

    PROCEDURE pr_benifit_msg_cutofftime;

    PROCEDURE pr_benifit_other_on_day;

    PROCEDURE pr_benifit_swift_on_day;

    PROCEDURE pr_benifit_vcb_on_day;

    PROCEDURE pr_benifit_ibps_on_day;

    PROCEDURE pr_benifit_msg_yesterday;

    PROCEDURE pr_benifit_ibps_by_account(p_date7 NUMBER, p_account_no VARCHAR2);

    PROCEDURE pr_benifit_internal_by_account(p_date7 NUMBER, p_account_no VARCHAR2);

      PROCEDURE pr_load_tw_txn(p_batch_no NUMBER, p_date DATE);--#20170214 lOCTX ADD FOR TUNING;

      PROCEDURE pr_unload_tw_txn(p_batch_no NUMBER);--#20170214 lOCTX ADD FOR TUNING;
    PROCEDURE pr_benifit_internal_txn_eod(p_date7 NUMBER);--#20171225 Loctx add fix loi thu huong giao dich phat sinh luc chay batch

END;

/
