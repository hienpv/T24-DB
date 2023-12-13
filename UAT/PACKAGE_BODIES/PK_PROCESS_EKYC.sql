--------------------------------------------------------
--  DDL for Package Body PK_PROCESS_EKYC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."PK_PROCESS_EKYC" AS

  PROCEDURE RELATED_ACCOUNT_EKYC AS
  BEGIN
    FOR data_rec IN (
      select * 
      from bk_account_info 
      where cif_no in (
        select cif_no 
        from bc_user_info a
        where status='ACTV' and remark ='EKYC' and create_time between sysdate-31 and sysdate
        and user_id not in (select user_id from bc_related_account where status='ACTV')
      ) and status = 'ACTV' and acct_type='CA'
    )
    LOOP
      insert into BC_RELATED_ACCOUNT( RELATION_ID, USER_ID, ACCT_NO, ACCT_TYPE, SUB_ACCT_TYPE, IS_MASTER, ALIAS, STATUS, 
		  CREATE_BY, CREATE_TIME, UPDATE_BY) values 
      (SEQ_RELATION_ID.nextval, (select user_id from bc_user_info where cif_no=data_rec.cif_no and status='ACTV'), data_rec.acct_no,
      data_rec.acct_type, null, 'N', (substr(data_rec.acct_no, length(data_rec.acct_no) -2, length(data_rec.acct_no))), 'ACTV', -1, sysdate, null);
    END LOOP;
    commit;
  END RELATED_ACCOUNT_EKYC;


END PK_PROCESS_EKYC;

/
