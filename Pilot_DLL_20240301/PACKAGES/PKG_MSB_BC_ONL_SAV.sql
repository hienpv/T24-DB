--------------------------------------------------------
--  DDL for Package PKG_MSB_BC_ONL_SAV
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "PKG_MSB_BC_ONL_SAV" IS

TYPE RECORD_ONL_SAV IS RECORD (
     CATEGORY VARCHAR2(50),
     CURRENTCY VARCHAR2(50) NOT NULL := 'VND',
     AMOUNT VARCHAR2(50),
     LANGUAGE VARCHAR2(50) NOT NULL := 'vi' /* Support vi and en */
);   
TYPE MYCURSOR IS REF CURSOR;

RESULT_SUCC CONSTANT NUMBER := 0; -- SUCCESS
RESULT_EXCP CONSTANT NUMBER := -1; -- EXCEPTION

PRATEN CONSTANT VARCHAR2(20) := 'PRATEN';
BALRT2 CONSTANT VARCHAR2(20) := 'BALRT2';
BALRT3 CONSTANT VARCHAR2(20) := 'BALRT3';
BALRT4 CONSTANT VARCHAR2(20) := 'BALRT4';
BALRT5 CONSTANT VARCHAR2(20) := 'BALRT5';
BALRT6 CONSTANT VARCHAR2(20) := 'BALRT6';
BALRT7 CONSTANT VARCHAR2(20) := 'BALRT7';
BALRT8 CONSTANT VARCHAR2(20) := 'BALRT8';
BALRT9 CONSTANT VARCHAR2(20) := 'BALRT9';
BALRT0 CONSTANT VARCHAR2(20) := 'BALRT0';
BALRTA CONSTANT VARCHAR2(20) := 'BALRTA';
BALRTB CONSTANT VARCHAR2(20) := 'BALRTB';

FUNCTION FN_GET_INR_SAV(CATEGORY IN VARCHAR2,
     CURRENTCY IN VARCHAR2,
     AMOUNT IN VARCHAR2,
     LANGUAGE IN VARCHAR2,
     CHANNEL_CODE IN VARCHAR2) RETURN NUMBER;

END PKG_MSB_BC_ONL_SAV;

/