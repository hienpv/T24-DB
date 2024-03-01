--------------------------------------------------------
--  DDL for Package CSPKS_UTIL
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKS_UTIL" 
IS

      FUNCTION fn_get_seq (p_name              IN     VARCHAR2,
                           p_key               IN     VARCHAR2,
                           p_seq                  OUT VARCHAR2,
                           p_auto_num_length   IN     NUMBER,
                           p_err_code             OUT VARCHAR2)
          RETURN NUMBER;

END;

/
