--------------------------------------------------------
--  DDL for Package KEEP_DATA_HIS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "KEEP_DATA_HIS" AS 
 
      PROCEDURE keep_his_bc_user_info; 
    function fn_partition_is_earlier(i_part_tab_name varchar2, i_partition_position number, i_ref_date in date) return number;

END KEEP_DATA_HIS;

/
