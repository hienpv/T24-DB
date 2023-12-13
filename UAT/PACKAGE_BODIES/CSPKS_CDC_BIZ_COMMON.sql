--------------------------------------------------------
--  DDL for Package Body CSPKS_CDC_BIZ_COMMON
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKS_CDC_BIZ_COMMON" 
IS
/*----------------------------------------------------------------------------------------------------
 ** Module   :
 ** and is copyrighted by FSS.
 **
 **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
 **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
 **    graphic, optic recording or otherwise, translated in any language or computer language,
 **    without the prior written permission of Financial Software Solutions. JSC.
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  Loctx      09-SEP-2014    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/

    FUNCTION fn_get_account_status_code(p_status VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE
    IS
        l_status_code VARCHAR2(10);
    BEGIN
        l_status_code := CASE TRIM (p_status)
                            WHEN '1' THEN 'ACTV'--ACTIVE
                            WHEN '2' THEN 'CLOS'--CLOSED
                            WHEN '3' THEN 'MATU'--MATURED
                            WHEN '4' THEN 'ACTV'--New Record
                            WHEN '5' THEN 'ACTZ'--Active zero balance
                            WHEN '6' THEN 'REST'--RESTRICTED
                            WHEN '7' THEN 'NOPO'--NO POST
                            WHEN '8' THEN 'COUN'--Code unavailable
                            WHEN '9' THEN 'DORM'--DORMANT
                            ELSE ''
                         END;
        return l_status_code;


    END;

    FUNCTION fn_get_actype_code(p_actype VARCHAR2)
    RETURN VARCHAR2 RESULT_CACHE
    IS
        l_actype_code VARCHAR2(10);
    BEGIN
        l_actype_code := CASE TRIM (p_actype)
                            WHEN 'S' THEN 'SA'--Saving
                            ELSE 'CA'
                         END;
        RETURN l_actype_code;

    END;


    PROCEDURE pr_lnhist_sync_trncode_filter(out_ret OUT NUMBER, in_lntran IN NUMBER)
    IS
    BEGIN
        IF in_lntran IN (
                912,993,990,914,922,915,121,101,962,974,988,42,41,976,35,15,30,
                62,61,23,22,21,145,143,497,496,781,889,926,102,811,812,906,43,964,148
             ) THEN
            out_ret := 1;
        ELSE
            out_ret := 0;
        END IF;
    END;

END cspks_cdc_biz_common;

/
