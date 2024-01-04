--------------------------------------------------------
--  File created - Wednesday-December-20-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_CDMAST
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_CDMAST" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"BEFORE_STATUS" NUMBER(30,10), 
	"BEFORE_ACNAME" VARCHAR2(500 BYTE), 
	"BEFORE_CIFNO" NUMBER(30,10), 
	"ACCTNO" NUMBER(30,10), 
	"STATUS" NUMBER(30,10), 
	"ACNAME" VARCHAR2(500 BYTE), 
	"CDTCOD" VARCHAR2(500 BYTE), 
	"CIFNO" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_CDMEMO
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_CDMEMO" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(500 BYTE), 
	"ACCTNO" NUMBER(30,10), 
	"CURTYP" VARCHAR2(4000 BYTE), 
	"CBAL" NUMBER(30,10), 
	"ACCINT" NUMBER(30,10), 
	"PENAMT" NUMBER(30,10), 
	"HOLD" NUMBER(30,10), 
	"WDRWH" NUMBER(30,10), 
	"CDNUM" NUMBER(30,10), 
	"STATUS" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_CDTNEW
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_CDTNEW" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"BANKNO" NUMBER(30,10), 
	"BRN" VARCHAR2(50 BYTE), 
	"CURTYP" VARCHAR2(500 BYTE), 
	"CIFNO" NUMBER(30,10), 
	"ORGBAL" NUMBER(30,10), 
	"CBAL" NUMBER(30,10), 
	"ACCINT" NUMBER(30,10), 
	"PENAMT" NUMBER(30,10), 
	"HOLD" NUMBER(30,10), 
	"WDRWH" NUMBER(30,10), 
	"CDNUM" NUMBER(30,10), 
	"ISSDT" NUMBER(30,10), 
	"MATDT" NUMBER(30,10), 
	"RNWCTR" NUMBER(30,10), 
	"STATUS" NUMBER(30,10), 
	"ACNAME" VARCHAR2(500 BYTE), 
	"ACCTNO" NUMBER(30,10), 
	"TYPE" VARCHAR2(500 BYTE), 
	"RATE" NUMBER(30,10), 
	"RENEW" VARCHAR2(500 BYTE), 
	"DACTN" NUMBER(30,10), 
	"CDTERM" NUMBER(30,10), 
	"CDMUID" VARCHAR2(500 BYTE), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE, 
	"CDTERMCODE" VARCHAR2(50 BYTE)
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_CFMAST
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_CFMAST" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"CFCIFN" NUMBER(30,10), 
	"CFSSCD" VARCHAR2(500 BYTE), 
	"CFSSNO" VARCHAR2(500 BYTE), 
	"CFBRNN" VARCHAR2(50 BYTE), 
	"CFNA1" VARCHAR2(500 BYTE), 
	"CFBIRD" NUMBER(30,10), 
	"CFBIRP" VARCHAR2(500 BYTE), 
	"CFCITZ" VARCHAR2(500 BYTE), 
	"CFINDI" VARCHAR2(500 BYTE), 
	"TAXCOD" VARCHAR2(500 BYTE), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_CFTNEW
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_CFTNEW" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"CFCIFN" NUMBER(30,10), 
	"CFSNME" VARCHAR2(500 BYTE), 
	"CFBUST" VARCHAR2(500 BYTE), 
	"CFOFFR" VARCHAR2(500 BYTE), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_DDMAST
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_DDMAST" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"BEFORE_STATUS" NUMBER(30,10), 
	"BEFORE_ACNAME" VARCHAR2(500 BYTE), 
	"BEFORE_CIFNO" NUMBER(30,10), 
	"BEFORE_BRANCH" VARCHAR2(50 BYTE), 
	"BEFORE_ODLIMT" NUMBER(30,10), 
	"ACCTNO" NUMBER(30,10), 
	"STATUS" NUMBER(30,10), 
	"ACNAME" VARCHAR2(500 BYTE), 
	"CIFNO" NUMBER(30,10), 
	"BRANCH" VARCHAR2(50 BYTE), 
	"ODLIMT" NUMBER(30,10), 
	"DLA7" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_DDMEMO
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_DDMEMO" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"BEFORE_CIFNO" NUMBER(30,10), 
	"CIFNO" NUMBER(30,10), 
	"ACCTNO" NUMBER(30,10), 
	"STATUS" NUMBER(30,10), 
	"HOLD" NUMBER(30,10), 
	"CBAL" NUMBER(30,10), 
	"ODLIMT" NUMBER(30,10), 
	"ACNAME" VARCHAR2(500 BYTE), 
	"DLA7" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_DDTNEW
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_DDTNEW" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"BEFORE_CIFNO" NUMBER(30,10), 
	"BRANCH" VARCHAR2(50 BYTE), 
	"ACCTNO" NUMBER(30,10), 
	"ACTYPE" VARCHAR2(500 BYTE), 
	"DDCTYP" VARCHAR2(500 BYTE), 
	"CIFNO" NUMBER(30,10), 
	"STATUS" NUMBER(30,10), 
	"HOLD" NUMBER(30,10), 
	"CBAL" NUMBER(30,10), 
	"ODLIMT" NUMBER(30,10), 
	"RATE" NUMBER(30,10), 
	"ACNAME" VARCHAR2(500 BYTE), 
	"SCCODE" VARCHAR2(500 BYTE), 
	"DATOP7" NUMBER(30,10), 
	"ACCRUE" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_LNMAST
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_LNMAST" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"BEFORE_STATUS" VARCHAR2(50 BYTE), 
	"BEFORE_TYPE" VARCHAR2(50 BYTE), 
	"BEFORE_CIFNO" VARCHAR2(50 BYTE), 
	"BEFORE_ORGAMT" VARCHAR2(50 BYTE), 
	"BEFORE_TERM" VARCHAR2(50 BYTE), 
	"BEFORE_TMCODE" VARCHAR2(50 BYTE), 
	"BEFORE_PMTAMT" VARCHAR2(50 BYTE), 
	"BEFORE_FNLPMT" VARCHAR2(50 BYTE), 
	"BEFORE_RATE" VARCHAR2(50 BYTE), 
	"ACCTNO" VARCHAR2(50 BYTE), 
	"STATUS" VARCHAR2(50 BYTE), 
	"TYPE" VARCHAR2(50 BYTE), 
	"CIFNO" VARCHAR2(50 BYTE), 
	"ORGAMT" VARCHAR2(50 BYTE), 
	"TERM" VARCHAR2(50 BYTE), 
	"TMCODE" VARCHAR2(50 BYTE), 
	"PMTAMT" VARCHAR2(50 BYTE), 
	"FNLPMT" VARCHAR2(50 BYTE), 
	"RATE" VARCHAR2(50 BYTE), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_LNMEMO
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_LNMEMO" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"ACCINT" VARCHAR2(50 BYTE), 
	"CURTYP" VARCHAR2(50 BYTE), 
	"CBAL" VARCHAR2(50 BYTE), 
	"BILPRN" VARCHAR2(50 BYTE), 
	"BILINT" VARCHAR2(50 BYTE), 
	"BILLC" VARCHAR2(50 BYTE), 
	"BILESC" VARCHAR2(50 BYTE), 
	"BILOC" VARCHAR2(50 BYTE), 
	"BILMC" VARCHAR2(50 BYTE), 
	"DRLIMT" VARCHAR2(50 BYTE), 
	"HOLD" VARCHAR2(50 BYTE), 
	"COMACC" VARCHAR2(50 BYTE), 
	"OTHCHG" VARCHAR2(50 BYTE), 
	"ACCTNO" VARCHAR2(50 BYTE), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_LNTNEW
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_LNTNEW" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"BRN" VARCHAR2(10 BYTE), 
	"ACCINT" VARCHAR2(50 BYTE), 
	"CIFNO" VARCHAR2(50 BYTE), 
	"LNNUM" VARCHAR2(50 BYTE), 
	"ACCTNO" VARCHAR2(50 BYTE), 
	"PURCOD" VARCHAR2(50 BYTE), 
	"CURTYP" VARCHAR2(50 BYTE), 
	"ORGAMT" VARCHAR2(50 BYTE), 
	"CBAL" VARCHAR2(50 BYTE), 
	"YSOBAL" VARCHAR2(50 BYTE), 
	"BILLCO" VARCHAR2(50 BYTE), 
	"FREQ" VARCHAR2(50 BYTE), 
	"IPFREQ" VARCHAR2(50 BYTE), 
	"FULLDT" VARCHAR2(50 BYTE), 
	"STATUS" VARCHAR2(50 BYTE), 
	"ODIND" VARCHAR2(50 BYTE), 
	"BILESC" VARCHAR2(50 BYTE), 
	"BILOC" VARCHAR2(50 BYTE), 
	"BILMC" VARCHAR2(50 BYTE), 
	"BILPRN" VARCHAR2(50 BYTE), 
	"BILINT" VARCHAR2(50 BYTE), 
	"BILLC" VARCHAR2(50 BYTE), 
	"PMTAMT" VARCHAR2(50 BYTE), 
	"FNLPMT" VARCHAR2(50 BYTE), 
	"DRLIMT" VARCHAR2(50 BYTE), 
	"HOLD" VARCHAR2(50 BYTE), 
	"ACCMLC" VARCHAR2(50 BYTE), 
	"COMACC" VARCHAR2(50 BYTE), 
	"OTHCHG" VARCHAR2(50 BYTE), 
	"ACNAME" VARCHAR2(50 BYTE), 
	"TYPE" VARCHAR2(50 BYTE), 
	"DATOPN" VARCHAR2(50 BYTE), 
	"MATDT" VARCHAR2(50 BYTE), 
	"FRELDT" VARCHAR2(50 BYTE), 
	"RATE" VARCHAR2(50 BYTE), 
	"TERM" VARCHAR2(50 BYTE), 
	"TMCODE" VARCHAR2(50 BYTE), 
	"BEFORE_ACCTNO" VARCHAR2(50 BYTE), 
	"BEFORE_CIFNO" VARCHAR2(50 BYTE), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_TMTRAN
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_TMTRAN" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(500 BYTE), 
	"TMTXCD" VARCHAR2(500 BYTE), 
	"TMRESV07" VARCHAR2(500 BYTE), 
	"TMDORC" VARCHAR2(500 BYTE), 
	"TMTXAMT" NUMBER(30,10), 
	"TMGLCUR" VARCHAR2(500 BYTE), 
	"TMORGAMT" NUMBER(30,10), 
	"TMEFTH" VARCHAR2(500 BYTE), 
	"TMACCTNO" VARCHAR2(50 BYTE), 
	"TMTELLID" VARCHAR2(500 BYTE), 
	"TMTXSEQ" VARCHAR2(50 BYTE), 
	"TMTXSTAT" VARCHAR2(500 BYTE), 
	"TMHOSTTXCD" NUMBER(30,10), 
	"TMAPPTYPE" VARCHAR2(500 BYTE), 
	"TMEQVTRN" VARCHAR2(500 BYTE), 
	"TMIBTTRN" VARCHAR2(500 BYTE), 
	"TMSUMTRN" VARCHAR2(500 BYTE), 
	"TMENTDT7" NUMBER(30,10), 
	"TMEFFDT7" NUMBER(30,10), 
	"TMSSEQ" VARCHAR2(50 BYTE), 
	"TMTIMENT" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_TMTRAN_10_2023
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_TMTRAN_10_2023" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(500 BYTE), 
	"TMTXCD" VARCHAR2(500 BYTE), 
	"TMRESV07" VARCHAR2(500 BYTE), 
	"TMDORC" VARCHAR2(500 BYTE), 
	"TMTXAMT" NUMBER(30,10), 
	"TMGLCUR" VARCHAR2(500 BYTE), 
	"TMORGAMT" NUMBER(30,10), 
	"TMEFTH" VARCHAR2(500 BYTE), 
	"TMACCTNO" VARCHAR2(50 BYTE), 
	"TMTELLID" VARCHAR2(500 BYTE), 
	"TMTXSEQ" VARCHAR2(50 BYTE), 
	"TMTXSTAT" VARCHAR2(500 BYTE), 
	"TMHOSTTXCD" NUMBER(30,10), 
	"TMAPPTYPE" VARCHAR2(500 BYTE), 
	"TMEQVTRN" VARCHAR2(500 BYTE), 
	"TMIBTTRN" VARCHAR2(500 BYTE), 
	"TMSUMTRN" VARCHAR2(500 BYTE), 
	"TMENTDT7" NUMBER(30,10), 
	"TMEFFDT7" NUMBER(30,10), 
	"TMSSEQ" NUMBER(30,10), 
	"TMTIMENT" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Table T24_CDD_LOG_TMTRAN_FAIL
--------------------------------------------------------

  CREATE TABLE "T24_CDD_LOG_TMTRAN_FAIL" 
   (	"ID" NUMBER(30,10), 
	"OPERATION_TYPE" VARCHAR2(1 BYTE), 
	"TMTXCD" VARCHAR2(500 BYTE), 
	"TMRESV07" VARCHAR2(500 BYTE), 
	"TMDORC" VARCHAR2(500 BYTE), 
	"TMTXAMT" NUMBER(30,10), 
	"TMGLCUR" VARCHAR2(500 BYTE), 
	"TMORGAMT" NUMBER(30,10), 
	"TMEFTH" VARCHAR2(500 BYTE), 
	"TMACCTNO" VARCHAR2(50 BYTE), 
	"TMTELLID" VARCHAR2(500 BYTE), 
	"TMTXSEQ" VARCHAR2(50 BYTE), 
	"TMTXSTAT" VARCHAR2(500 BYTE), 
	"TMHOSTTXCD" NUMBER(30,10), 
	"TMAPPTYPE" VARCHAR2(500 BYTE), 
	"TMEQVTRN" VARCHAR2(500 BYTE), 
	"TMIBTTRN" VARCHAR2(500 BYTE), 
	"TMSUMTRN" VARCHAR2(500 BYTE), 
	"TMENTDT7" NUMBER(30,10), 
	"TMEFFDT7" NUMBER(30,10), 
	"TMSSEQ" VARCHAR2(50 BYTE), 
	"TMTIMENT" NUMBER(30,10), 
	"NO_SEQ" NUMBER(30,10), 
	"INFO_STATUS" NUMBER(1,0), 
	"INFO_DESC" VARCHAR2(4000 BYTE), 
	"CREATE_TIME" TIMESTAMP (6), 
	"CREATE_DATE" DATE
   );
--------------------------------------------------------
--  DDL for Index SYS_C0025605
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025605" ON "T24_CDD_LOG_CDMAST" ("ID");
  
--------------------------------------------------------
--  DDL for Index SYS_C0025606
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025606" ON "T24_CDD_LOG_CDMEMO" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C0025607
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025607" ON "T24_CDD_LOG_CDTNEW" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C0025609
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025609" ON "T24_CDD_LOG_CFMAST" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C0025610
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025610" ON "T24_CDD_LOG_CFTNEW" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C0021671
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0021671" ON "T24_CDD_LOG_DDMAST" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C0025608
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025608" ON "T24_CDD_LOG_DDMEMO" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C0021673
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0021673" ON "T24_CDD_LOG_DDTNEW" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C00216733
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C00216733" ON "T24_CDD_LOG_LNMAST" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C00216732
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C00216732" ON "T24_CDD_LOG_LNMEMO" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C00216731
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C00216731" ON "T24_CDD_LOG_LNTNEW" ("ID") ;
--------------------------------------------------------
--  DDL for Index SYS_C0025611
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025611" ON "T24_CDD_LOG_TMTRAN" ("ID");
--------------------------------------------------------
--  DDL for Index SYS_C0025612
--------------------------------------------------------

  CREATE UNIQUE INDEX "SYS_C0025612" ON "T24_CDD_LOG_TMTRAN_FAIL" ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_CDMAST
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_CDMAST" ADD PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_CDMEMO
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_CDMEMO" ADD PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_CDTNEW
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_CDTNEW" ADD PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_CFMAST
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_CFMAST" ADD PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_CFTNEW
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_CFTNEW" ADD PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_DDMAST
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_DDMAST" ADD CONSTRAINT "SYS_C0021671" PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_DDMEMO
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_DDMEMO" ADD PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_DDTNEW
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_DDTNEW" ADD CONSTRAINT "SYS_C0021673" PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_LNMAST
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_LNMAST" ADD CONSTRAINT "SYS_C00216733" PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_LNMEMO
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_LNMEMO" ADD CONSTRAINT "SYS_C00216732" PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_LNTNEW
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_LNTNEW" ADD CONSTRAINT "SYS_C00216731" PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_TMTRAN
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_TMTRAN" ADD PRIMARY KEY ("ID");
--------------------------------------------------------
--  Constraints for Table T24_CDD_LOG_TMTRAN_FAIL
--------------------------------------------------------

  ALTER TABLE "T24_CDD_LOG_TMTRAN_FAIL" ADD PRIMARY KEY ("ID");
  
  
  --------------------------------------------------------
--  File created - Wednesday-December-20-2023   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_CDMAST_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_CDMAST_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 698881 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_CDMEMO_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_CDMEMO_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 2583241 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_CDTNEW_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_CDTNEW_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 641104 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_CFMAST_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_CFMAST_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 18001 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_CFTNEW_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_CFTNEW_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 421 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_DDMAST_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_DDMAST_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 4873341 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_DDMEMO_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_DDMEMO_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 5614167 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_DDTNEW_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_DDTNEW_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 90610 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_LNMAST_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_LNMAST_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 101 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_LNMEMO_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_LNMEMO_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 217461 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_LNTNEW_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_LNTNEW_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 11441 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_TMTRAN_FAIL_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_TMTRAN_FAIL_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 81 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;
--------------------------------------------------------
--  DDL for Sequence T24_CDD_LOG_TMTRAN_ID_SEQ
--------------------------------------------------------

   CREATE SEQUENCE  "T24IBS"."T24_CDD_LOG_TMTRAN_ID_SEQ"  MINVALUE 1 MAXVALUE 999999999999999999999999 INCREMENT BY 1 START WITH 330071 CACHE 20 NOORDER  NOCYCLE  NOPARTITION ;

