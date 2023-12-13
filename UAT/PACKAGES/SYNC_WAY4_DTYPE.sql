--------------------------------------------------------
--  DDL for Package SYNC_WAY4_DTYPE
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "T24IBS"."SYNC_WAY4_DTYPE" is

/**
* Project:         W4Kernel
* Description:     Standard data types
* @headcom
*/

  Counter                              integer;
  RecordID                            integer;

  RecordIdt                          varchar2(255);
  RecordIdtLength       constant Counter        %Type   := 255;

  Tag                                     varchar2 (1);
  TagLength              constant Counter       %Type   := 1;

  StoredNameLength     constant Counter     %Type   := 32;

  Name                                   varchar2 (255);
  NameLength          constant Counter      %Type   := 255;

  String                       varchar2 (255);
  StringLength        constant Counter      %Type   := 255;

  ErrorMessage                        varchar2 (4000);
  ErrorMessageLength     constant Counter       %Type   := 3900;

  LongStr                              varchar2 (4000);
  LongStrLength          constant Counter       %Type   := 3900;

  XMLString                          varchar2 (32740);
  XMLStringLength       constant Counter        %Type   := 32740;

  CurrentTime                          date;
  CurrentDate                          date;
  CurrentTimestamp                 timestamp (3);

  LongInteger                          integer;
  BigDecimal                            number;
  XMLclob                              clob;
  BinaryBlob                            blob;
  LongData                            long;
  ClobData                            clob;
  NClobData                          nclob;
end sync_way4_dtype;

/
