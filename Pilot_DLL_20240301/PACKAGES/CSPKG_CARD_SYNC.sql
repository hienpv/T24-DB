--------------------------------------------------------
--  DDL for Package CSPKG_CARD_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKG_CARD_SYNC" 
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

      PROCEDURE pr_card_info_sync(dm_operation_type in CHAR,
                              orchestrate_eccard in VARCHAR,
                              orchestrate_eccsts in VARCHAR,
                              orchestrate_eccif in VARCHAR,
                              orchestrate_ecdtef in NUMBER,
                              orchestrate_ecdex4 in NUMBER,
                              orchestrate_ecctyp in VARCHAR,
                              orchestrate_ecdtis in NUMBER,
                              orchestrate_ecshnm in VARCHAR,
                              orchestrate_eccrty in VARCHAR);

END; -- Package spec

/
