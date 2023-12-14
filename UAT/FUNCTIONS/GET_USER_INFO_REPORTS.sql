--------------------------------------------------------
--  DDL for Function GET_USER_INFO_REPORTS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE FUNCTION "GET_USER_INFO_REPORTS" (
              p_USER_ID IN BB_USER_INFO.USER_ID%TYPE
      )
   RETURN SYS_REFCURSOR
AS
   c_direct_reports SYS_REFCURSOR;
BEGIN
   
   OPEN c_direct_reports 
   FOR 
        select USER_NAME, NICK  
        from BB_USER_INFO 
        where user_id = p_USER_ID or 1 = p_user_id ;
         
   RETURN c_direct_reports;

END;

/
