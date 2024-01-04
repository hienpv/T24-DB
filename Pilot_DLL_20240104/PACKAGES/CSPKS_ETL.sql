--------------------------------------------------------
--  DDL for Package CSPKS_ETL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKS_ETL" 
IS
/*----------------------------------------------------------------------------------------------------
 ** Module: FTP SYSTEM
 ** and is copyrighted by FSS.
 **
 **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
 **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
 **    graphic, optic recording or otherwise, translated in any language or computer language,
 **    without the prior written permission of Financial Software Solutions. JSC.
 **
 **  MODIFICATION HISTORY
 **  Person            Date                Comments
 **  Loctx            22/08/2013               Created
 **
 ** (c) 2013 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_main_etl_begin(
                            p_user             IN VARCHAR2,
                            p_job_name    IN VARCHAR2,
                            p_etl_date         IN NUMBER,
                            p_process_id       OUT VARCHAR2
                            );

    PROCEDURE pr_etl_begin( p_user             IN VARCHAR2,
                            p_job_name        IN VARCHAR2,
                            p_process_group     IN VARCHAR2,
                            p_process_id       IN VARCHAR2,
                            p_etl_date         IN NUMBER
                            );

    PROCEDURE pr_etl_end(   p_job_name       IN VARCHAR2,
                            p_process_id       IN VARCHAR2,
                            p_success_ind    IN CHAR,
                            p_error         IN VARCHAR2,
                            p_description  IN VARCHAR2
                            );
    PROCEDURE prc_gather_table (
                                  p_n_tream_run number
                                );


END;

/
