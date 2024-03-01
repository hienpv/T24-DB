--------------------------------------------------------
--  DDL for Package Body CSPKS_CDC_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKS_CDC_UTIL" 
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
 **  Loctx      13-11-2014    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_log_sync_error(p_source_id VARCHAR2,
                                p_source_table VARCHAR2,
                                p_target_table VARCHAR2,
                                p_unique_id_in_source VARCHAR2,
                                p_operation_type VARCHAR2,
                                p_log_text VARCHAR2)
    IS
    BEGIN
            INSERT INTO cstb_cdc_log (source_id, source_table,
                                    unique_id_in_source, target_table,
                                    operation_type,
                                    log_date, log_text,
                                    status)
                        VALUES(p_source_id, p_source_table,
                                    p_unique_id_in_source, p_target_table,
                                    p_operation_type, SYSDATE, p_log_text, 'N');
    END;

END;

/
