--------------------------------------------------------
--  DDL for Package Body W4PKG_DATA_ETL_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "W4PKG_DATA_ETL_SYNC" 
IS
/*----------------------------------------------------------------------------------------------------
 ** Module   : ODS SYSTEM
 ** and is copyrighted by FSS.
 **
 **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
 **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
 **    graphic, optic recording or otherwise, translated in any language or computer language,
 **    without the prior written permission of Financial Software Solutions. JSC.
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  LocTX      31/10/2014    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/
    pkgctx    plog.log_ctx;
    logrow    tlogdebug%ROWTYPE;

BEGIN
    SELECT   *
    INTO   logrow
    FROM   tlogdebug
    WHERE   ROWNUM <= 1;

    pkgctx      :=
    plog.init('w4pkg_data_etl_sync',
        plevel => logrow.loglevel,
        plogtable => ( logrow.log4table = 'Y' ),
        palert => ( logrow.log4alert = 'Y' ),
        ptrace => ( logrow.log4trace = 'Y' ) );
END;

/
