--------------------------------------------------------
--  DDL for Package CSPKG_TRANSACTION_ETL_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKG_TRANSACTION_ETL_SYNC" 
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
     **  LocTX      31-10-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/

      PROCEDURE pr_backup_hist(p_etl_date NUMBER);

      PROCEDURE pr_cdhist_sync;

      PROCEDURE pr_ddhist_sync;

      PROCEDURE pr_lnhist_sync ;

      PROCEDURE pr_reset_txn_sequence ;

      PROCEDURE pr_ddhist_new_cif_sync( p_etl_date IN VARCHAR2);

      PROCEDURE pr_cdhist_new_cif_sync(p_etl_date IN VARCHAR2);

      PROCEDURE pr_update_benifit_info( p_etl_date IN NUMBER);


      PROCEDURE pr_lnhist_new_cif_sync (p_etl_date IN VARCHAR2);


      PROCEDURE proc_ddft_transaction_by_date (p_etl_date NUMBER);

    PROCEDURE pr_benifit_batch_txn(p_date7 NUMBER);

END;

/
