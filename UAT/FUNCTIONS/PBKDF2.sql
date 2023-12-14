--------------------------------------------------------
--  DDL for Function PBKDF2
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "PBKDF2" 
  ( p_password IN VARCHAR2
  , p_salt IN VARCHAR2
  , p_count IN INTEGER
  , p_key_length IN INTEGER )
RETURN VARCHAR2
IS
    l_block_count INTEGER;
    l_last RAW(32767);
    l_xorsum RAW(32767);
    l_result RAW(32767);
BEGIN
    l_block_count := CEIL(p_key_length / 20);  -- use 20 bytes for SHA1, 32 for SHA256, 64 for SHA512

    FOR i IN 1..l_block_count LOOP
        l_last := UTL_RAW.CONCAT(UTL_RAW.CAST_TO_RAW(p_salt), UTL_RAW.CAST_FROM_BINARY_INTEGER(i, UTL_RAW.BIG_ENDIAN));
        l_xorsum := NULL;

        FOR j IN 1..p_count LOOP
            l_last := DBMS_CRYPTO.MAC(l_last, DBMS_CRYPTO.HMAC_SH1, UTL_RAW.CAST_TO_RAW(p_password));
            -- use HMAC_SH256 for SHA256, HMAC_SH512 for SHA512

            IF l_xorsum IS NULL THEN
                l_xorsum := l_last;
            ELSE
                l_xorsum := UTL_RAW.BIT_XOR(l_xorsum, l_last);
            END IF;
        END LOOP;

        l_result := UTL_RAW.CONCAT(l_result, l_xorsum);
    END LOOP;

    RETURN RAWTOHEX(UTL_RAW.SUBSTR(l_result, 1, p_key_length));
END pbkdf2;

/
