--------------------------------------------------------
--  DDL for Package Body ENCRYPT_PASWD
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "ENCRYPT_PASWD" is
 G_CHARACTER_SET VARCHAR2(10) := 'AL32UTF8';
  G_STRING VARCHAR2(32) := '12345678901234567890123456789012';
  G_KEY RAW(250) := utl_i18n.string_to_raw
                      ( data => G_STRING,
                        dst_charset => G_CHARACTER_SET );
  G_ENCRYPTION_TYPE PLS_INTEGER := dbms_crypto.encrypt_aes256 
                                    + dbms_crypto.chain_cbc 
                                    + dbms_crypto.pad_pkcs5;
  ------------------------------------------------------------------------
  --Encrypt a password 
  --Salt the password
  ------------------------------------------------------------------------
  FUNCTION encrypt_val( p_val IN VARCHAR2 ) RETURN RAW
  IS
    l_val RAW(32) := UTL_I18N.STRING_TO_RAW( p_val, G_CHARACTER_SET );
    l_encrypted RAW(32);
  BEGIN
    l_val := utl_i18n.string_to_raw
              ( data => p_val,
                dst_charset => G_CHARACTER_SET );

    l_encrypted := dbms_crypto.encrypt
                   ( src => l_val,
                     typ => G_ENCRYPTION_TYPE,
                     key => G_KEY );

    RETURN l_encrypted;
  END encrypt_val;
end encrypt_paswd;

/
