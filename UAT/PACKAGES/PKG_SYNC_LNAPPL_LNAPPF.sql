--------------------------------------------------------
--  DDL for Package PKG_SYNC_LNAPPL_LNAPPF
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PKG_SYNC_LNAPPL_LNAPPF" 
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
     **  HaoNS      09-SEP-2014    Created
     ** (c) 2014 by Financial Software Solutions. JSC.
     ----------------------------------------------------------------------------------------------------*/

      PROCEDURE sync_lnappl;
      PROCEDURE sync_lnappf;
      PROCEDURE sync_lnappl_lnappf;

END;

/
