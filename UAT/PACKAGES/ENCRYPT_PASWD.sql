--------------------------------------------------------
--  DDL for Package ENCRYPT_PASWD
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "ENCRYPT_PASWD" is

FUNCTION encrypt_val( p_val IN VARCHAR2 ) RETURN RAW;

end encrypt_paswd;

/
