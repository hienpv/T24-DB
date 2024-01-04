--------------------------------------------------------
--  DDL for Package CSPKS_CDC_T24_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKS_CDC_T24_UTIL" AS 

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
                              
                              );
							  
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
                              
                              );

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
                              
                              ) ;
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
                              
                              );   

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
                              
                              );      
                              
	 PROCEDURE PR_T24_CDD_LOG_DDTNEW (
                            dm_operation_type IN CHAR,
                                orchestrate_before_cifno IN NUMBER, --#20160921 LocTx add for cif change
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
                              
                              );  
                              
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
                              
                              );  
                              
     PROCEDURE PR_T24_CDD_LOG_CFTNEW (
                               dm_operation_type in CHAR,
                                      orchestrate_cfcifn in NUMBER,
                                      orchestrate_cfsnme in VARCHAR,
                                      orchestrate_cfbust in VARCHAR,
                                      orchestrate_cfoffr in VARCHAR   , 
							  NO_SEQ  IN NUMBER,
                              INFO_STATUS IN NUMBER,			
							  INFO_DESC  IN   VARCHAR2 
                              
                              );                            
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
                              
                              )  ;

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
                              
                              )  ;   

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
                              
                              )  ;                              
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
                              
                              );  
                              
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
                              
                              );  
                              
                              
END CSPKS_CDC_T24_UTIL;

/
