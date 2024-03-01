--------------------------------------------------------
--  DDL for Package Body SYNC_HANDLE_CDTNEW
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."SYNC_HANDLE_CDTNEW" 
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



/*----------------------------------------------------------------------------------------------------
 **  Description:
 **
 **  MODIFICATION HISTORY
 **  Person      Date           Comments
 **  HaoNS      31/10/2014    Created

----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_cdtnew_sync 
    IS
        l_cifno VARCHAR2(30);
        l_err_desc VARCHAR2(250);
        l_taget_table VARCHAR2(100);
        
        
        cursor cdmast_list is select * from sync_cdtnew_handl;
        
                       
    item       cdmast_list%Rowtype;
    
    BEGIN
                      
         open cdmast_list;
        LOOP
            FETCH cdmast_list INTO item;
            EXIT WHEN cdmast_list%NOTFOUND;
            begin
              -- call sync CDTNEW
              cspkg_account_sync.pr_cdtnew_sync('I',item.bankno,
              item.brn, item.curtyp, item.cifno,item.orgbal, item.cbal,
              item.accint, item.penamt, item.hold, item.wdrwh, item.cdnum
              , item.issdt, item.matdt, item.rnwctr, item.status, item.acname,
              item.acctno, item.type, item.rate, item.renew, item.dactn,
               item.cdterm, item.cdmuid);
               l_cifno := item.cifno;
            
           exception
               WHEN others THEN 
                 rollback;
              dbms_output.put_line('Error!');  
            end;
            END LOOP;
    
            close cdmast_list;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            l_err_desc := SUBSTR( sqlerrm, 1, 200);
    END;

END;

/
