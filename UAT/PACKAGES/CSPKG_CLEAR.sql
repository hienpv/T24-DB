--------------------------------------------------------
--  DDL for Package CSPKG_CLEAR
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."CSPKG_CLEAR" 
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
     **  LocTX      05-12-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/

      PROCEDURE pr_delete_cb_cdc(p_etl_date NUMBER);

      PROCEDURE pr_delete_tranmap(p_etl_date NUMBER);

END;

/
