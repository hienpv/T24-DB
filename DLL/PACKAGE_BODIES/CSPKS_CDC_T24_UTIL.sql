--------------------------------------------------------
--  DDL for Package Body CSPKS_CDC_T24_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKS_CDC_T24_UTIL" AS

  PROCEDURE PR_T24_CDD_LOG_CDMAST (
                              dm_operation_type in CHAR,
                              orchestrate_before_status in NUMBER,
                              orchestrate_before_acname IN VARCHAR2,
                              orchestrate_before_cifno IN NUMBER, 
                              orchestrate_acctno IN NUMBER,
                              orchestrate_status IN NUMBER,
                              orchestrate_acname IN VARCHAR2,
                              orchestrate_cdtcod IN CHAR,
                              orchestrate_cifno IN NUMBER  ,
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC   IN  VARCHAR2 
                              
                              ) AS
  BEGIN
       INSERT INTO T24_CDD_LOG_CDMAST (
                            ID                ,       
							OPERATION_TYPE    ,
                            BEFORE_STATUS     ,
                            BEFORE_ACNAME     ,
                            BEFORE_CIFNO      ,
                            ACCTNO            ,
                            STATUS            ,
                            ACNAME            ,
                            CDTCOD            ,
                            CIFNO             ,
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_CDMAST_ID_SEQ.NEXTVAL ,
						      dm_operation_type              ,  
                              orchestrate_before_status    ,
                             SUBSTR( orchestrate_before_acname, 1, 500)     ,
                              orchestrate_before_cifno     ,
                              orchestrate_acctno           ,
                              orchestrate_status           ,
                             SUBSTR( orchestrate_acname, 1, 500)            ,
                             SUBSTR( orchestrate_cdtcod, 1, 500)             ,
                              orchestrate_cifno            ,
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							   SUBSTR( INFO_DESC, 1, 4000)                        ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
                        
                        
  END PR_T24_CDD_LOG_CDMAST;

  PROCEDURE PR_T24_CDD_LOG_CDMEMO (
                              dm_operation_type in CHAR,
                              orchestrate_acctno in NUMBER,
                              orchestrate_curtyp in VARCHAR,
                              orchestrate_cbal in NUMBER,
                              orchestrate_accint in NUMBER,
                              orchestrate_penamt in NUMBER,
                              orchestrate_hold in NUMBER,
                              orchestrate_wdrwh in NUMBER,
                              orchestrate_cdnum in NUMBER,
                              orchestrate_status in NUMBER,
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC   IN  VARCHAR2 
                              
                              ) AS
  BEGIN
     INSERT INTO T24_CDD_LOG_CDMEMO (
                            ID                ,       
                           OPERATION_TYPE , 
                           ACCTNO         ,
                           CURTYP         ,
                           CBAL           ,
                           ACCINT         ,
                           PENAMT         ,
                           HOLD           ,
                           WDRWH          ,
                           CDNUM          ,
                           STATUS         ,                         
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_CDMEMO_ID_SEQ.NEXTVAL ,
						      dm_operation_type   ,
                              orchestrate_acctno  ,
                             SUBSTR( orchestrate_curtyp, 1, 500)        ,
                              orchestrate_cbal    ,
                              orchestrate_accint  ,
                              orchestrate_penamt  ,
                              orchestrate_hold    ,
                              orchestrate_wdrwh   ,
                              orchestrate_cdnum   ,
                              orchestrate_status  , 
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							   SUBSTR( INFO_DESC, 1, 4000)                      ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_CDMEMO;

  PROCEDURE PR_T24_CDD_LOG_CDTNEW (
                             dm_operation_type in CHAR,
                        orchestrate_bankno in NUMBER,
                        orchestrate_brn in VARCHAR,
                        orchestrate_curtyp in VARCHAR,
                        orchestrate_cifno in NUMBER,
                        orchestrate_orgbal in NUMBER,
                        orchestrate_cbal in NUMBER,
                        orchestrate_accint in NUMBER,
                        orchestrate_penamt in NUMBER,
                        orchestrate_hold in NUMBER,
                        orchestrate_wdrwh in NUMBER,
                        orchestrate_cdnum in NUMBER,
                        orchestrate_issdt in NUMBER,
                        orchestrate_matdt in NUMBER,
                        orchestrate_rnwctr in NUMBER,
                        orchestrate_status in NUMBER,
                        orchestrate_acname in VARCHAR,
                        orchestrate_acctno in NUMBER,
                        orchestrate_type in VARCHAR,
                        orchestrate_rate in NUMBER,
                        orchestrate_renew in VARCHAR,
                        orchestrate_dactn in NUMBER,
                        orchestrate_cdterm in NUMBER,
                        orchestrate_cdmuid in VARCHAR,
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC   IN  VARCHAR2 ,
                              orchestrate_cdtermcode in VARCHAR
                              ) AS
  BEGIN
    
      INSERT INTO T24_CDD_LOG_CDTNEW (
                            ID                ,       
                        OPERATION_TYPE   ,
                        BANKNO          ,
                        BRN             ,
                        CURTYP          ,
                        CIFNO           ,
                        ORGBAL          ,
                        CBAL            ,
                        ACCINT          ,
                        PENAMT          ,
                        HOLD            ,
                        WDRWH           ,
                        CDNUM           ,
                        ISSDT           ,
                        MATDT           ,
                        RNWCTR          ,
                        STATUS          ,
                        ACNAME          ,
                        ACCTNO          ,
                        TYPE            ,
                        RATE            ,
                        RENEW           ,
                        DACTN           ,
                        CDTERM          ,
                        CDMUID          ,                              
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_CDTNEW_ID_SEQ.NEXTVAL ,
   					   	       dm_operation_type   , 
                        orchestrate_bankno    ,
                        orchestrate_brn       ,
                        orchestrate_curtyp    ,
                        orchestrate_cifno     ,
                        orchestrate_orgbal    ,
                        orchestrate_cbal      ,
                        orchestrate_accint    ,
                        orchestrate_penamt    ,
                        orchestrate_hold      ,
                        orchestrate_wdrwh     ,
                        orchestrate_cdnum     ,
                        orchestrate_issdt     ,
                        orchestrate_matdt     ,
                        orchestrate_rnwctr    ,
                        orchestrate_status    ,
                        orchestrate_acname    ,
                        orchestrate_acctno    ,
                        orchestrate_type      ,
                        orchestrate_rate      ,
                        orchestrate_renew     ,
                        orchestrate_dactn     ,
                        orchestrate_cdterm    ,
                        orchestrate_cdmuid    ,
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
                        
  END PR_T24_CDD_LOG_CDTNEW;

  PROCEDURE PR_T24_CDD_LOG_DDMAST (
                             dm_operation_type in CHAR,
                            orchestrate_before_status IN NUMBER,
                            orchestrate_before_acname  IN VARCHAR2,
                            orchestrate_before_cifno IN NUMBER, 
                            orchestrate_before_branch IN VARCHAR, 
                            orchestrate_before_odlimt IN NUMBER,  
                            orchestrate_acctno IN NUMBER,
                            orchestrate_status IN NUMBER,
                            orchestrate_acname IN VARCHAR2,
                            orchestrate_cifno IN NUMBER, 
                            orchestrate_branch IN VARCHAR, 
                            orchestrate_odlimt IN NUMBER, 
                            orchestrate_dla7 IN NUMBER , 
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC   IN  VARCHAR2 
                              
                              ) AS
  BEGIN
     INSERT INTO T24_CDD_LOG_DDMAST (
                            ID                ,       
                           OPERATION_TYPE  ,
                            BEFORE_STATUS   ,
                            BEFORE_ACNAME   ,
                            BEFORE_CIFNO    ,
                            BEFORE_BRANCH   ,
                            BEFORE_ODLIMT   ,
                            ACCTNO          ,
                            STATUS          ,
                            ACNAME          ,
                            CIFNO           ,
                            BRANCH          ,
                            ODLIMT          ,
                            DLA7            ,                                             
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_DDMAST_ID_SEQ.NEXTVAL ,
                             dm_operation_type           ,
                            orchestrate_before_status   ,
                            orchestrate_before_acname   ,
                            orchestrate_before_cifno    ,
                            orchestrate_before_branch   ,
                            orchestrate_before_odlimt   ,
                            orchestrate_acctno          ,
                            orchestrate_status          ,
                            orchestrate_acname          ,
                            orchestrate_cifno           ,
                            orchestrate_branch          ,
                            orchestrate_odlimt          ,
                            orchestrate_dla7            , 					      
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_DDMAST;

  PROCEDURE PR_T24_CDD_LOG_DDMEMO (
                                dm_operation_type in CHAR,
                                orchestrate_before_cifno IN NUMBER,  
                                orchestrate_cifno IN NUMBER,  
                                orchestrate_acctno IN NUMBER,
                                orchestrate_status IN NUMBER,
                                orchestrate_hold IN NUMBER,
                                orchestrate_cbal IN NUMBER,
                                orchestrate_odlimt IN NUMBER,
                                orchestrate_acname IN VARCHAR2,
                                orchestrate_dla7 IN NUMBER, 
							    NO_SEQ  IN NUMBER,
                                INFO_STATUS IN NUMBER,			
							    INFO_DESC   IN  VARCHAR2 
                              
                              ) AS
  BEGIN
    INSERT INTO T24_CDD_LOG_DDMEMO (
                                ID                ,       
                                OPERATION_TYPE   ,  
								BEFORE_CIFNO     ,
								CIFNO            ,
                                ACCTNO           ,
                                STATUS           ,
                                HOLD             ,
                                CBAL             ,
                                ODLIMT           ,
                                ACNAME           ,
                                DLA7             ,                                          
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_DDMEMO_ID_SEQ.NEXTVAL ,
  					             dm_operation_type         ,
                                orchestrate_before_cifno  ,
                                orchestrate_cifno         ,
                                orchestrate_acctno        ,
                                orchestrate_status        ,
                                orchestrate_hold          ,
                                orchestrate_cbal          ,
                                orchestrate_odlimt        ,
                                orchestrate_acname        ,
                                orchestrate_dla7          ,  
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_DDMEMO;

  PROCEDURE PR_T24_CDD_LOG_DDTNEW (
                            dm_operation_type IN CHAR,
                                orchestrate_before_cifno IN NUMBER,  
                                orchestrate_branch  IN VARCHAR,
                                orchestrate_acctno IN NUMBER,
                                orchestrate_actype IN VARCHAR,
                                orchestrate_ddctyp IN VARCHAR,
                                orchestrate_cifno  IN NUMBER,
                                orchestrate_status  IN NUMBER,
                                orchestrate_hold IN NUMBER,
                                orchestrate_cbal IN NUMBER,
                                orchestrate_odlimt IN NUMBER,
                                orchestrate_rate  IN NUMBER,
                                orchestrate_acname IN VARCHAR,
                                orchestrate_sccode IN VARCHAR,
                                orchestrate_datop7 IN NUMBER,
                                orchestrate_accrue IN NUMBER, 
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC  IN   VARCHAR2 
                              
                              ) AS
  BEGIN
     INSERT INTO T24_CDD_LOG_DDTNEW (
                            ID                ,       
                           								OPERATION_TYPE  , 
                                BEFORE_CIFNO    ,
                                BRANCH          ,
                                ACCTNO          ,
                                ACTYPE          ,
                                DDCTYP          ,
                                CIFNO           ,
                                STATUS          ,
                                HOLD            ,
                                CBAL            ,
                                ODLIMT          ,
                                RATE            ,
                                ACNAME          ,
                                SCCODE          ,
                                DATOP7          ,
                                ACCRUE          ,                                                                    
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_DDTNEW_ID_SEQ.NEXTVAL ,
  					           dm_operation_type          ,
                                orchestrate_before_cifno   ,
                                orchestrate_branch         ,
                                orchestrate_acctno         ,
                                orchestrate_actype         ,
                                orchestrate_ddctyp         ,
                                orchestrate_cifno          ,
                                orchestrate_status         ,
                                orchestrate_hold           ,
                                orchestrate_cbal           ,
                                orchestrate_odlimt         ,
                                orchestrate_rate           ,
                                orchestrate_acname         ,
                                orchestrate_sccode         ,
                                orchestrate_datop7         ,
                                orchestrate_accrue         , 
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_DDTNEW;

  PROCEDURE PR_T24_CDD_LOG_CFMAST (
                                 dm_operation_type in CHAR,
                                  orchestrate_cfcifn in NUMBER,
                                  orchestrate_cfsscd in VARCHAR2,
                                  orchestrate_cfssno in VARCHAR2,
                                  orchestrate_cfbrnn in VARCHAR2,
                                  orchestrate_cfna1 in VARCHAR2,
                                  orchestrate_cfbird in NUMBER,
                                  orchestrate_cfbirp in VARCHAR2,
                                  orchestrate_cfcitz in VARCHAR2,
                                  orchestrate_cfindi in VARCHAR2,
                                  orchestrate_taxcod in VARCHAR2 , 
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC  IN   VARCHAR2 
                              
                              ) AS
  BEGIN
   INSERT INTO T24_CDD_LOG_CFMAST (
                            ID                ,       
                             								  OPERATION_TYPE  , 
                                  CFCIFN          ,
                                  CFSSCD          ,
                                  CFSSNO          ,
                                  CFBRNN          ,
                                  CFNA1           ,
                                  CFBIRD          ,
                                  CFBIRP          ,
                                  CFCITZ          ,
                                  CFINDI          ,
                                  TAXCOD          ,                                                                    
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_CFMAST_ID_SEQ.NEXTVAL ,
  					              dm_operation_type    ,
                                  orchestrate_cfcifn   ,
                                  orchestrate_cfsscd   ,
                                  orchestrate_cfssno   ,
                                  orchestrate_cfbrnn   ,
                                  orchestrate_cfna1    ,
                                  orchestrate_cfbird   ,
                                  orchestrate_cfbirp   ,
                                  orchestrate_cfcitz   ,
                                  orchestrate_cfindi   ,
                                  orchestrate_taxcod   , 
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_CFMAST;

  PROCEDURE PR_T24_CDD_LOG_CFTNEW (
                               dm_operation_type in CHAR,
                                      orchestrate_cfcifn in NUMBER,
                                      orchestrate_cfsnme in VARCHAR,
                                      orchestrate_cfbust in VARCHAR,
                                      orchestrate_cfoffr in VARCHAR   , 
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC  IN   VARCHAR2 
                              
                              ) AS
  BEGIN
   INSERT INTO T24_CDD_LOG_CFTNEW (
                                   ID                ,       
                                   OPERATION_TYPE   ,
                                      CFCIFN           ,
                                      CFSNME           ,
                                      CFBUST           ,
                                      CFOFFR           ,
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_CFTNEW_ID_SEQ.NEXTVAL ,
                                      dm_operation_type     ,
                                      orchestrate_cfcifn   ,
                                      orchestrate_cfsnme   ,
                                      orchestrate_cfbust   ,
                                      orchestrate_cfoffr   ,					      
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_CFTNEW;

PROCEDURE PR_T24_CDD_LOG_LNMAST (
                            
                            dm_operation_type in CHAR,
                            orchestrate_before_status in VARCHAR2,
                            orchestrate_before_type IN VARCHAR2,
                            orchestrate_before_cifno IN VARCHAR2, 
                            orchestrate_before_orgamt IN VARCHAR2,
                            orchestrate_before_term IN VARCHAR2,
                            orchestrate_before_tmcode IN VARCHAR2,
                            orchestrate_before_pmtamt IN VARCHAR2,
                            orchestrate_before_fnlpmt IN VARCHAR2,
                            orchestrate_before_rate IN VARCHAR2,
                            orchestrate_acctno IN VARCHAR2,
                            orchestrate_status IN VARCHAR2,
                            orchestrate_type IN VARCHAR2,
                            orchestrate_cifno IN VARCHAR2, 
                            orchestrate_orgamt IN VARCHAR2,
                            orchestrate_term IN VARCHAR2,
                            orchestrate_tmcode IN VARCHAR2,
                            orchestrate_pmtamt IN VARCHAR2,
                            orchestrate_fnlpmt IN VARCHAR2,
                            orchestrate_rate IN VARCHAR2 ,
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC  IN   VARCHAR2 
                              
                              ) AS
  BEGIN
  INSERT INTO T24IBS.T24_CDD_LOG_LNMAST
 (ID, OPERATION_TYPE, BEFORE_STATUS, BEFORE_TYPE, BEFORE_CIFNO, BEFORE_ORGAMT, BEFORE_TERM, BEFORE_TMCODE, BEFORE_PMTAMT, BEFORE_FNLPMT, BEFORE_RATE, ACCTNO, STATUS, "TYPE", CIFNO, ORGAMT, TERM, TMCODE, PMTAMT, FNLPMT, RATE, NO_SEQ, INFO_STATUS, INFO_DESC, CREATE_TIME, CREATE_DATE)
    
                        VALUES(
						      T24_CDD_LOG_LNMAST_ID_SEQ.NEXTVAL ,
                             dm_operation_type           ,
                            orchestrate_before_status ,
                            orchestrate_before_type   ,
                            orchestrate_before_cifno  ,
                            orchestrate_before_orgamt ,
                            orchestrate_before_term   ,
                            orchestrate_before_tmcode ,
                            orchestrate_before_pmtamt ,
                            orchestrate_before_fnlpmt ,
                            orchestrate_before_rate ,
                            orchestrate_acctno  ,
                            orchestrate_status  ,
                            orchestrate_type    ,
                            orchestrate_cifno   ,
                            orchestrate_orgamt  ,
                            orchestrate_term    ,
                            orchestrate_tmcode  ,
                            orchestrate_pmtamt  ,
                            orchestrate_fnlpmt  ,
                            orchestrate_rate    ,
							
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_LNMAST;


PROCEDURE PR_T24_CDD_LOG_LNTNEW (
                            
                            dm_operation_type in CHAR,
							
                              orchestrate_brn in VARCHAR,
                            orchestrate_accint in VARCHAR,
                            orchestrate_cifno in VARCHAR,
                            orchestrate_lnnum in VARCHAR,
                            orchestrate_acctno in VARCHAR,
                            orchestrate_purcod in VARCHAR,
                            orchestrate_curtyp in VARCHAR,
                            orchestrate_orgamt in VARCHAR,
                            orchestrate_cbal in VARCHAR,
                            orchestrate_ysobal in VARCHAR,
                            orchestrate_billco in VARCHAR,
                            orchestrate_freq in VARCHAR,
                            orchestrate_ipfreq in VARCHAR,
                            orchestrate_fulldt in VARCHAR,
                            orchestrate_status in VARCHAR,
                            orchestrate_odind in VARCHAR,
                            orchestrate_bilesc in VARCHAR,
                            orchestrate_biloc in VARCHAR,
                            orchestrate_bilmc in VARCHAR,
                            orchestrate_bilprn in VARCHAR,
                            orchestrate_bilint in VARCHAR,
                            orchestrate_billc in VARCHAR,
                            orchestrate_pmtamt in VARCHAR,
                            orchestrate_fnlpmt in VARCHAR,
                            orchestrate_drlimt in VARCHAR,
                            orchestrate_hold in VARCHAR,
                            orchestrate_accmlc in VARCHAR,
                            orchestrate_comacc in VARCHAR,
                            orchestrate_othchg in VARCHAR,
                            orchestrate_acname in VARCHAR,
                            orchestrate_type in VARCHAR,
                            orchestrate_datopn in VARCHAR,
                            orchestrate_matdt in VARCHAR,
                            orchestrate_freldt in VARCHAR,
                            orchestrate_rate in VARCHAR,
                            orchestrate_term in VARCHAR,
                            orchestrate_tmcode in VARCHAR,
                            orchestrate_before_acctno in VARCHAR,
                            orchestrate_before_cifno in VARCHAR ,
							
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC  IN   VARCHAR2 
                              
                              ) AS
  BEGIN
  INSERT INTO T24IBS.T24_CDD_LOG_LNTNEW
(ID, OPERATION_TYPE, BRN, ACCINT, CIFNO, LNNUM, ACCTNO, PURCOD, CURTYP, ORGAMT, CBAL, YSOBAL, BILLCO, FREQ, IPFREQ, FULLDT, STATUS, ODIND, BILESC, BILOC, BILMC, BILPRN, BILINT, BILLC, PMTAMT, FNLPMT, DRLIMT, "HOLD", ACCMLC, COMACC, OTHCHG, ACNAME, "TYPE", DATOPN, MATDT, FRELDT, RATE, TERM, TMCODE, BEFORE_ACCTNO, BEFORE_CIFNO, NO_SEQ, INFO_STATUS, INFO_DESC, CREATE_TIME, CREATE_DATE)
                        VALUES(
						      T24_CDD_LOG_LNTNEW_ID_SEQ.NEXTVAL ,
                             dm_operation_type           ,
                             orchestrate_brn   ,
                            orchestrate_accint ,
                            orchestrate_cifno  ,
                            orchestrate_lnnum  ,
                            orchestrate_acctno ,
                            orchestrate_purcod ,
                            orchestrate_curtyp ,
                            orchestrate_orgamt ,
                            orchestrate_cbal   ,
                            orchestrate_ysobal ,
                            orchestrate_billco ,
                            orchestrate_freq   ,
                            orchestrate_ipfreq ,
                            orchestrate_fulldt ,
                            orchestrate_status ,
                            orchestrate_odind  ,
                            orchestrate_bilesc ,
                            orchestrate_biloc  ,
                            orchestrate_bilmc  ,
                            orchestrate_bilprn ,
                            orchestrate_bilint ,
                            orchestrate_billc  ,
                            orchestrate_pmtamt ,
                            orchestrate_fnlpmt ,
                            orchestrate_drlimt ,
                            orchestrate_hold   ,
                            orchestrate_accmlc ,
                            orchestrate_comacc ,
                            orchestrate_othchg ,
                            orchestrate_acname ,
                            orchestrate_type   ,
                            orchestrate_datopn ,
                            orchestrate_matdt  ,
                            orchestrate_freldt ,
                            orchestrate_rate ,
                            orchestrate_term ,
                            orchestrate_tmcode  ,
                            orchestrate_before_acctno  ,
                            orchestrate_before_cifno  ,
							
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_LNTNEW;
                         

PROCEDURE PR_T24_CDD_LOG_LNMEMO (
                            
                            dm_operation_type in CHAR,
							
                            orchestrate_accint IN VARCHAR,
                            orchestrate_curtyp IN VARCHAR,
                            orchestrate_cbal IN VARCHAR,
                            orchestrate_bilprn IN VARCHAR ,
                            orchestrate_bilint IN VARCHAR,
                            orchestrate_billc IN VARCHAR,
                            orchestrate_bilesc IN VARCHAR ,
                            orchestrate_biloc IN VARCHAR,
                            orchestrate_bilmc IN VARCHAR,
                            orchestrate_drlimt IN VARCHAR,
                            orchestrate_hold IN VARCHAR,
                            orchestrate_comacc IN VARCHAR,
                            orchestrate_othchg IN VARCHAR,
                            orchestrate_acctno IN VARCHAR,
							
							  NO_SEQ  IN VARCHAR2,
                              INFO_STATUS IN VARCHAR2,			
							  INFO_DESC  IN   VARCHAR2 
                              
                              ) AS
  BEGIN
  
INSERT INTO T24IBS.T24_CDD_LOG_LNMEMO
(ID, OPERATION_TYPE, ACCINT, CURTYP, CBAL, BILPRN, BILINT, BILLC, BILESC, BILOC, BILMC, DRLIMT, "HOLD", COMACC, OTHCHG, ACCTNO, NO_SEQ, INFO_STATUS, INFO_DESC, CREATE_TIME, CREATE_DATE)

                        VALUES(
						      T24_CDD_LOG_LNMEMO_ID_SEQ.NEXTVAL ,
                             dm_operation_type           ,
                            orchestrate_accint ,
                            orchestrate_curtyp ,
                            orchestrate_cbal   ,
                            orchestrate_bilprn ,
                            orchestrate_bilint ,
                            orchestrate_billc  ,
                            orchestrate_bilesc ,
                            orchestrate_biloc  ,
                            orchestrate_bilmc  ,
                            orchestrate_drlimt ,
                            orchestrate_hold   ,
                            orchestrate_comacc ,
                            orchestrate_othchg ,
                            orchestrate_acctno ,
							
							
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_LNMEMO;
                   
                                            
  PROCEDURE PR_T24_CDD_LOG_TMTRAN (
                              dm_operation_type in CHAR,
                                  orchestrate_tmtxcd in VARCHAR,
                                  orchestrate_tmresv07 in VARCHAR,
                                  orchestrate_tmdorc in CHAR,
                                  orchestrate_tmtxamt in NUMBER,
                                  orchestrate_tmglcur in VARCHAR,
                                  orchestrate_tmorgamt in NUMBER,
                                  orchestrate_tmefth in VARCHAR2,
                                  orchestrate_tmacctno in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                  orchestrate_tmtellid in VARCHAR,
                                  orchestrate_tmtxseq  in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                  orchestrate_tmtxstat in VARCHAR,
                                  orchestrate_tmhosttxcd in NUMBER,
                                  orchestrate_tmapptype in CHAR,
                                  orchestrate_tmeqvtrn in CHAR,
                                  orchestrate_tmibttrn in CHAR,
                                  orchestrate_tmsumtrn in CHAR,
                                  orchestrate_tmentdt7 in NUMBER,
                                  orchestrate_tmeffdt7 in NUMBER,
                                  orchestrate_tmsseq in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                  orchestrate_tmtiment in NUMBER  , 
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC   IN  VARCHAR2 
                              
                              ) AS
  BEGIN
   INSERT INTO T24_CDD_LOG_TMTRAN (
                                   ID                ,       
                                  OPERATION_TYPE   ,
                                  TMTXCD           ,
                                  TMRESV07         ,
                                  TMDORC           ,
                                  TMTXAMT          ,
                                  TMGLCUR          ,
                                  TMORGAMT         ,
                                  TMEFTH           ,
                                  TMACCTNO         ,
                                  TMTELLID         ,
                                  TMTXSEQ          ,
                                  TMTXSTAT         ,
                                  TMHOSTTXCD       ,
                                  TMAPPTYPE        ,
                                  TMEQVTRN         ,
                                  TMIBTTRN         ,
                                  TMSUMTRN         ,
                                  TMENTDT7         ,
                                  TMEFFDT7         ,
                                  TMSSEQ           ,
                                  TMTIMENT         ,                                     
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_TMTRAN_ID_SEQ.NEXTVAL ,
  					                dm_operation_type        ,
                                  orchestrate_tmtxcd       ,
                                  orchestrate_tmresv07     ,
                                  orchestrate_tmdorc       ,
                                  orchestrate_tmtxamt      ,
                                  orchestrate_tmglcur      ,
                                  orchestrate_tmorgamt     ,
                                  orchestrate_tmefth       ,
                                  orchestrate_tmacctno     ,
                                  orchestrate_tmtellid     ,
                                  orchestrate_tmtxseq      ,
                                  orchestrate_tmtxstat     ,
                                  orchestrate_tmhosttxcd   ,
                                  orchestrate_tmapptype    ,
                                  orchestrate_tmeqvtrn     ,
                                  orchestrate_tmibttrn     ,
                                  orchestrate_tmsumtrn     ,
                                  orchestrate_tmentdt7     ,
                                  orchestrate_tmeffdt7     ,
                                  orchestrate_tmsseq       ,
                                  orchestrate_tmtiment     ,
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_TMTRAN;

  PROCEDURE PR_T24_CDD_LOG_TMTRAN_FAIL (
                               dm_operation_type in CHAR,
                                  orchestrate_tmtxcd in VARCHAR,
                                  orchestrate_tmresv07 in VARCHAR,
                                  orchestrate_tmdorc in CHAR,
                                  orchestrate_tmtxamt in NUMBER,
                                  orchestrate_tmglcur in VARCHAR,
                                  orchestrate_tmorgamt in NUMBER,
                                  orchestrate_tmefth in VARCHAR2,
                                  orchestrate_tmacctno in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                  orchestrate_tmtellid in VARCHAR,
                                  orchestrate_tmtxseq in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                  orchestrate_tmtxstat in VARCHAR,
                                  orchestrate_tmhosttxcd in NUMBER,
                                  orchestrate_tmapptype in CHAR,
                                  orchestrate_tmeqvtrn in CHAR,
                                  orchestrate_tmibttrn in CHAR,
                                  orchestrate_tmsumtrn in CHAR,
                                  orchestrate_tmentdt7 in NUMBER,
                                  orchestrate_tmeffdt7 in NUMBER,
                                  orchestrate_tmsseq in VARCHAR, -- quandh3 2023 T24 change type number to varchar
                                  orchestrate_tmtiment in NUMBER  , 
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC   IN  VARCHAR2 
                              
                              ) AS
  BEGIN
    INSERT INTO T24_CDD_LOG_TMTRAN_FAIL (
                            ID                ,       
                           	  OPERATION_TYPE   ,
                                  TMTXCD           ,
                                  TMRESV07         ,
                                  TMDORC           ,
                                  TMTXAMT          ,
                                  TMGLCUR          ,
                                  TMORGAMT         ,
                                  TMEFTH           ,
                                  TMACCTNO         ,
                                  TMTELLID         ,
                                  TMTXSEQ          ,
                                  TMTXSTAT         ,
                                  TMHOSTTXCD       ,
                                  TMAPPTYPE        ,
                                  TMEQVTRN         ,
                                  TMIBTTRN         ,
                                  TMSUMTRN         ,
                                  TMENTDT7         ,
                                  TMEFFDT7         ,
                                  TMSSEQ           ,
                                  TMTIMENT         ,                                                 
							NO_SEQ            ,
							INFO_STATUS           ,
							INFO_DESC         ,
							CREATE_TIME       ,
	                        CREATE_DATE      
	                         
							)
                        VALUES(
						      T24_CDD_LOG_TMTRAN_FAIL_ID_SEQ.NEXTVAL ,
  					         dm_operation_type        ,
                                  orchestrate_tmtxcd      ,
                                  orchestrate_tmresv07    ,
                                  orchestrate_tmdorc      ,
                                  orchestrate_tmtxamt     ,
                                  orchestrate_tmglcur     ,
                                  orchestrate_tmorgamt    ,
                                  orchestrate_tmefth      ,
                                  orchestrate_tmacctno    ,
                                  orchestrate_tmtellid    ,
                                  orchestrate_tmtxseq     ,
                                  orchestrate_tmtxstat    ,
                                  orchestrate_tmhosttxcd  ,
                                  orchestrate_tmapptype   ,
                                  orchestrate_tmeqvtrn    ,
                                  orchestrate_tmibttrn    ,
                                  orchestrate_tmsumtrn    ,
                                  orchestrate_tmentdt7    ,
                                  orchestrate_tmeffdt7    ,
                                  orchestrate_tmsseq      ,
                                  orchestrate_tmtiment    ,    
							  NO_SEQ                       ,
                              INFO_STATUS  		           ,
							  INFO_DESC                    ,
							  CURRENT_TIMESTAMP ,
							  SYSDATE		
			            );
  END PR_T24_CDD_LOG_TMTRAN_FAIL;

END CSPKS_CDC_T24_UTIL;

/
