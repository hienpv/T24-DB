--------------------------------------------------------
--  DDL for Procedure MSB_CLEAR_TRANS_SCHEDULE_BUFF
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "MSB_CLEAR_TRANS_SCHEDULE_BUFF" 
is
BEGIN
delete bk_tran_schedule_buffer where schedule_id in (
select distinct h.schdule_id from bc_transfer_history h, bk_tran_schedule s where h.is_schedule= 'Y' and h.oh_sign = 'O' and h.status = 'SCFH' and 
h.schdule_id = s.schdule_id and end_type = 'C');
delete bk_tran_schedule_buffer where schedule_id in (
select distinct h.schdule_id from bc_transfer_history h, bk_tran_schedule s where h.is_schedule= 'Y' and h.oh_sign = 'O' and h.status = 'SCFH' and 
h.schdule_id = s.schdule_id and is_lack_stop = 'Y');

END;

/
