--------------------------------------------------------
--  DDL for Package CSPKG_ERRNUMS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."CSPKG_ERRNUMS" 
/* Formatted on 8/4/2012 10:27:56 AM (QP5 v5.126) */
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
 **  TienPQ      09-SEP-2011    Created
 ** (c) 2008 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/
IS
    c_success                     CONSTANT NUMBER := 1;
    c_failed                      CONSTANT NUMBER := -1;
    c_process_later               CONSTANT NUMBER := 2; --20150808 ThanhNT add de xu ly process_later CDMEMO

    e_system_error exception;
    c_system_error                CONSTANT VARCHAR2 (6) := 'SY-001';

    c_process_success             CONSTANT CHAR (1) := 'S';
    c_process_fail                CONSTANT CHAR (1) := 'F';

END cspkg_errnums;

/
