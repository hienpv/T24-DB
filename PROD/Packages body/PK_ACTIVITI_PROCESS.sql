--------------------------------------------------------
--  DDL for Package Body PK_ACTIVITI_PROCESS
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."PK_ACTIVITI_PROCESS" AS
  PROCEDURE PROCESS_DATA_ACTIVITI
  AS
  BEGIN
--    FOR activiti_rec IN (
--      select * from ACT_HI_PROCINST where start_time_ < sysdate - 30  and rownum <= 1000 order by start_time_      
--    )
--    LOOP
--      delete from ACT_HI_COMMENT where task_id_ in (select id_ from ACT_HI_TASKINST where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_);
--      
--      delete from ACT_HI_ACTINST where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_HI_IDENTITYLINK 
--      where TASK_ID_ in (select id_ from ACT_HI_TASKINST where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_ and task_def_key_ like 'APPROVE_REJECT%');
--      
--      delete from ACT_HI_IDENTITYLINK where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_HI_DETAIL where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_; 
--      
--      delete from ACT_HI_VARINST where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_HI_TASKINST where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_HI_PROCINST where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_RU_VARIABLE where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_RU_IDENTITYLINK where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_RU_IDENTITYLINK 
--      where TASK_ID_ in (select id_ from ACT_RU_TASK where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_ and task_def_key_ like 'APPROVE_REJECT%');
--      
--      delete from ACT_RU_TASK where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--      delete from ACT_RU_EXECUTION where PROC_INST_ID_ = activiti_rec.PROC_INST_ID_;
--      
--    END LOOP;
--    delete from hotp where created_time < sysdate - 30 and rownum < 5000;
--    commit;
--    delete from wf_bb_process_id where create_time < sysdate - 30 and rownum < 1001;
--    commit;
    null;
  END;
END PK_ACTIVITI_PROCESS;

/
