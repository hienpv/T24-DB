--------------------------------------------------------
--  DDL for Package Body PK_MIGRATE_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."PK_MIGRATE_DATA" AS
  PROCEDURE P_BB_USER_PERMISSION_UPGRADE (
    P_MODULE_ID NUMBER,
    P_ROLE_ID NUMBER
  ) AS
  BEGIN
    insert into BB_USER_PERMISSION_UPGRADE (permission_id, corp_id, role_id, user_id, c_permission_id, module_id, action_type, create_by, create_by_mng, create_time, update_by, update_by_mng, update_time, CHANNEL_CODE)
    select SEQ_PERMISSION_ID.nextval, -- permission_id,
    corp_id, role_id, user_id, c_permission_id, module_id, action_type, create_by, create_by_mng, create_time, update_by, update_by_mng, update_time, CHANNEL_CODE   
    from (
    
      select -1 permission_id, a.corp_id, a.role_id, -1 user_id, b.permission_id c_permission_id, -1 module_id, a.action_type, -1 create_by, -1 create_by_mng, sysdate create_time, -1 update_by, -1 update_by_mng, sysdate update_time, 'IB' CHANNEL_CODE 
      from (
          select service_group_id, max(permission_id) permission_id, corp_id, role_id, max(c_permission_id) c_permission_id, action_type 
          from (
            select bcg.service_group_id, upu.* 
            from bb_user_permission_upgrade upu
            left join bb_corp_info bci on upu.corp_id = bci.corp_id
            left join bb_corp_group bcg on bci.group_id = bcg.group_id
            where upu.role_id=P_ROLE_ID and (upu.user_id is null or upu.user_id=-1) and upu.action_type='E' -- role maker
          ) a
          group by service_group_id, corp_id, role_id, action_type
      ) a
      left join (select * from BB_CORP_PERMISSION_UPGRADE where module_id=P_MODULE_ID) b on a.service_group_id = b.service_group_id
      where b.service_group_id is not null
    
    ) a
    where a.corp_id not in (
      select distinct corp_id from bb_user_permission_upgrade where role_id=P_ROLE_ID and user_id=-1 and c_permission_id in ( -- role maker
          select permission_id from BB_CORP_PERMISSION_UPGRADE where module_id=P_MODULE_ID)
    );
    commit;
  END;
END PK_MIGRATE_DATA;

/
