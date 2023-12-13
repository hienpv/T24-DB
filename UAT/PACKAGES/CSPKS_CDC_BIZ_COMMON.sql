--------------------------------------------------------
--  DDL for Package CSPKS_CDC_BIZ_COMMON
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKS_CDC_BIZ_COMMON" 
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
 **  Loctx      09-SEP-2014    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/


      FUNCTION fn_get_actype_code(p_actype VARCHAR2)
      RETURN VARCHAR2 RESULT_CACHE;

      FUNCTION fn_get_account_status_code(p_status VARCHAR2)
      RETURN VARCHAR2 RESULT_CACHE;

END;

/
