--------------------------------------------------------
--  DDL for Package Body CSPKG_GWSERVICE_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CSPKG_GWSERVICE_SYNC" 
/* Formatted on 05-Dec-2014 17:40:49 (QP5 v5.160) */
IS
/*----------------------------------------------------------------------------------------------------
    ** Module   : ODS SYSTEM
    ** and is copyrighted by FSS.
    **
    **    All rights reserved.  No part of this work may be reproduced, stored in a retrieval system,
    **    adopted or transmitted in any form or by any means, electronic, mechanical, photographic,
    **    graphic, optic recording or otherwise, translated in any language or computer language,
    **    without the prior written permission of Financial Software Solutions. JSC.
    **
    **  MODIFICATION HISTORY
    **  Person      Date           Comments
    **  HaoNS      06-DEC-2014    Created
    ** (c) 2014 by Financial Software Solutions. JSC.
----------------------------------------------------------------------------------------------------*/
    PROCEDURE pr_vcb_msg_content_sync ( dm_operation_type  IN CHAR,
                                        orchestrate_msg_id IN NUMBER,
                                        orchestrate_query_id IN NUMBER,
                                        orchestrate_msg_type   IN VARCHAR2,
                                        orchestrate_msg_direction IN VARCHAR2,
                                        orchestrate_branch_a  IN VARCHAR2,
                                        orchestrate_branch_b IN VARCHAR2,
                                        orchestrate_trans_date  IN DATE,
                                        orchestrate_value_date IN DATE,
                                        orchestrate_f20  IN VARCHAR2,
                                        orchestrate_f21 IN VARCHAR2,
                                        orchestrate_amount IN NUMBER,
                                        orchestrate_ccycd  IN CHAR,
                                        orchestrate_status  IN NUMBER,
                                        orchestrate_err_code IN NUMBER,
                                        orchestrate_department  IN VARCHAR2,
                                        orchestrate_header_content IN VARCHAR2,
                                        orchestrate_content  IN VARCHAR2,
                                        orchestrate_file_name IN VARCHAR2,
                                        orchestrate_foreign_bank IN VARCHAR2,
                                        orchestrate_trans_no IN VARCHAR2,
                                        orchestrate_rm_number IN VARCHAR2,
                                        orchestrate_receiving_time  IN DATE,
                                        orchestrate_sending_time  IN DATE,
                                        orchestrate_msg_src IN NUMBER,
                                        orchestrate_transdate  IN NUMBER,
                                        orchestrate_foreign_bank_name IN VARCHAR2,
                                        orchestrate_priority  IN NUMBER,
                                        orchestrate_print_sts IN NUMBER,
                                        orchestrate_product_type IN VARCHAR2,
                                        orchestrate_sibs_tellerid IN VARCHAR2)
    IS
        l_err_desc   VARCHAR2 (250);
    BEGIN
        IF dm_operation_type <> 'D'
        THEN
            MERGE INTO sync_cdc_vcb_msg_content c
             USING (SELECT orchestrate_msg_id AS msg_id,
                           orchestrate_query_id AS query_id,
                           orchestrate_msg_type AS msg_type,
                           orchestrate_msg_direction AS msg_direction,
                           orchestrate_branch_a AS branch_a,
                           orchestrate_branch_b AS branch_b,
                           orchestrate_trans_date AS trans_date,
                           orchestrate_value_date AS value_date,
                           TRIM(orchestrate_f20) AS f20,
                           orchestrate_f21 AS f21,
                           orchestrate_amount AS amount,
                           orchestrate_ccycd AS ccycd,
                           orchestrate_status AS status,
                           orchestrate_err_code AS err_code,
                           orchestrate_department AS department,
                           orchestrate_header_content AS header_content,
                           orchestrate_content AS content,
                           orchestrate_file_name AS file_name,
                           orchestrate_foreign_bank AS foreign_bank,
                           orchestrate_trans_no AS trans_no,
                           orchestrate_rm_number AS rm_number,
                           orchestrate_receiving_time AS receiving_time,
                           orchestrate_sending_time AS sending_time,
                           orchestrate_msg_src AS msg_src,
                           orchestrate_transdate AS transdate,
                           orchestrate_foreign_bank_name AS foreign_bank_name,
                           orchestrate_priority AS priority,
                           orchestrate_print_sts AS print_sts,
                           orchestrate_product_type AS product_type,
                           orchestrate_sibs_tellerid AS sibs_tellerid,
                           CASE
                               WHEN INSTRC (orchestrate_content,'52D',1,1) > 0
                               THEN gw_pk_vcb_report.vcb_get_swift_field (orchestrate_content,
                                                                         '52D',
                                                                         2,
                                                                        0,
                                                                        orchestrate_msg_type)
                               ELSE gw_pk_vcb_report.vcb_get_swift_field (
                                        orchestrate_content,
                                        '50K',
                                        1,
                                        0,
                                        orchestrate_msg_type)
                           END
                               AS send_name,
                           CASE
                               WHEN INSTRC (orchestrate_content,
                                            '52D',
                                            1,
                                            1) > 0
                               THEN
                                   REPLACE (
                                       REPLACE (
                                           REPLACE (
                                               SUBSTR (
                                                   gw_pk_vcb_report.vcb_get_swift_field (
                                                       orchestrate_content,
                                                       '52D',
                                                       1,
                                                       0,
                                                       orchestrate_msg_type),
                                                   1,
                                                   50),
                                               '.',
                                               ''),
                                           ' ',
                                           ''),
                                       '-',
                                       '')
                               ELSE
                                   REPLACE (
                                       REPLACE (
                                           REPLACE (
                                               SUBSTR (
                                                   gw_pk_vcb_report.vcb_get_swift_field (
                                                       orchestrate_content,
                                                       '50K',
                                                       2,
                                                       0,
                                                       orchestrate_msg_type),
                                                   1,
                                                   50),
                                               '.',
                                               ''),
                                           ' ',
                                           ''),
                                       '-',
                                       '')
                           END
                               AS send_account,
                           gw_pk_lib.get_swift_field (orchestrate_content,
                                                      '59',
                                                      2,
                                                      1,
                                                      orchestrate_msg_type)
                               AS recieve_name,
                           REPLACE (
                               REPLACE (
                                   REPLACE (
                                       SUBSTR (gw_pk_lib.get_swift_field (
                                                   orchestrate_content,
                                                   '59',
                                                   1,
                                                   1,
                                                   orchestrate_msg_type),
                                               1,
                                               50),
                                       '.',
                                       ''),
                                   ' ',
                                   ''),
                               '-',
                               '')
                               AS receive_account,
                           SUBSTR (gw_pk_lib.get_swift_field (
                                       orchestrate_content,
                                       '72',
                                       0,
                                       1,
                                       orchestrate_msg_type), 1, 300)
                               AS remark,
                           gw_pk_vcb_report.vcb_get_swift_field (
                               orchestrate_content,
                               '57D',
                               1,
                               0,
                               orchestrate_msg_type)
                               banks
                      FROM DUAL) a
                ON (c.msg_id = a.msg_id)--#20150526 Loctx remove  AND c.query_id = a.query_id (key la msg_id)
            WHEN MATCHED
            THEN
                UPDATE SET --c.msg_id = a.msg_id,
                           --c.query_id = a.query_id,
                           c.msg_type = a.msg_type,
                           c.msg_direction = a.msg_direction,
                           c.branch_a = a.branch_a,
                           c.branch_b = a.branch_b,
                           c.trans_date = a.trans_date,
                           c.value_date = a.value_date,
                           c.f20 = a.f20,
                           c.f21 = a.f21,
                           c.amount = a.amount,
                           c.ccycd = a.ccycd,
                           c.status = a.status,
                           c.err_code = a.err_code,
                           c.department = a.department,
                           c.header_content = a.header_content,
                           c.content = a.content,
                           c.file_name = a.file_name,
                           c.foreign_bank = a.foreign_bank,
                           c.trans_no = a.trans_no,
                           c.rm_number = a.rm_number,
                           c.receiving_time = a.receiving_time,
                           c.sending_time = a.sending_time,
                           c.msg_src = a.msg_src,
                           c.transdate = a.transdate,
                           c.foreign_bank_name = a.foreign_bank_name,
                           c.priority = a.priority,
                           c.print_sts = a.print_sts,
                           c.product_type = a.product_type,
                           c.sibs_tellerid = a.sibs_tellerid,
                           c.send_name = a.send_name,
                           c.send_account = a.send_account,
                           c.recieve_name = a.recieve_name,
                           c.receive_account = a.receive_account,
                           c.remark = a.remark,
                           c.banks = a.banks
            WHEN NOT MATCHED
            THEN
                INSERT (c.msg_id,
                        c.query_id,
                        c.msg_type,
                        c.msg_direction,
                        c.branch_a,
                        c.branch_b,
                        c.trans_date,
                        c.value_date,
                        c.f20,
                        c.f21,
                        c.amount,
                        c.ccycd,
                        c.status,
                        c.err_code,
                        c.department,
                        c.header_content,
                        c.content,
                        c.file_name,
                        c.foreign_bank,
                        c.trans_no,
                        c.rm_number,
                        c.receiving_time,
                        c.sending_time,
                        c.msg_src,
                        c.transdate,
                        c.foreign_bank_name,
                        c.priority,
                        c.print_sts,
                        c.product_type,
                        c.sibs_tellerid,
                        c.send_name,
                        c.send_account,
                        c.recieve_name,
                        c.receive_account,
                        c.remark,
                        c.banks)
                VALUES (a.msg_id,
                        a.query_id,
                        a.msg_type,
                        a.msg_direction,
                        a.branch_a,
                        a.branch_b,
                        a.trans_date,
                        a.value_date,
                        a.f20,
                        a.f21,
                        a.amount,
                        a.ccycd,
                        a.status,
                        a.err_code,
                        a.department,
                        a.header_content,
                        a.content,
                        a.file_name,
                        a.foreign_bank,
                        a.trans_no,
                        a.rm_number,
                        a.receiving_time,
                        a.sending_time,
                        a.msg_src,
                        a.transdate,
                        a.foreign_bank_name,
                        a.priority,
                        a.print_sts,
                        a.product_type,
                        a.sibs_tellerid,
                        a.send_name,
                        a.send_account,
                        a.recieve_name,
                        a.receive_account,
                        a.remark,
                        a.banks);
        END IF;

        COMMIT;
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN --#20150527 LocTX add khong can raise exception
            ROLLBACK;
            l_err_desc := SUBSTR (SQLERRM, 1, 200);

            cspks_cdc_util.pr_log_sync_error (
                'GWSERVICE',
                'VCB_MSG_CONTENT',
                'SYNC_CDC_VCB_MSG_CONTENT',
                orchestrate_msg_id || ',' || orchestrate_query_id,
                dm_operation_type,
                l_err_desc);
            COMMIT;
            --
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_err_desc := SUBSTR (SQLERRM, 1, 200);

            cspks_cdc_util.pr_log_sync_error (
                'GWSERVICE',
                'VCB_MSG_CONTENT',
                'SYNC_CDC_VCB_MSG_CONTENT',
                orchestrate_msg_id || ',' || orchestrate_query_id,
                dm_operation_type,
                l_err_desc);
            COMMIT;
            RAISE;
    END;

    PROCEDURE pr_ibps_msg_content_sync ( dm_operation_type IN CHAR,
                                        orchestrate_msg_id IN NUMBER,
                                        orchestrate_query_id IN NUMBER,
                                        orchestrate_file_name IN VARCHAR2,
                                        orchestrate_msg_direction IN VARCHAR2,
                                        orchestrate_trans_code IN VARCHAR2,
                                        orchestrate_gw_trans_num IN NUMBER,
                                        orchestrate_sibs_trans_num IN NUMBER,
                                        orchestrate_trans_date IN DATE,
                                        orchestrate_amount IN NUMBER,
                                        orchestrate_ccycd IN VARCHAR2,
                                        orchestrate_status IN NUMBER,
                                        orchestrate_err_code IN NUMBER,
                                        orchestrate_trans_description IN VARCHAR2,
                                        orchestrate_department IN VARCHAR2,
                                        orchestrate_content IN VARCHAR2,
                                        orchestrate_source_branch IN VARCHAR2,
                                        orchestrate_tad IN VARCHAR2,
                                        orchestrate_pre_tad IN VARCHAR2,
                                        orchestrate_rm_number IN VARCHAR2,
                                        orchestrate_pretran_code IN VARCHAR2,
                                        orchestrate_pretrans_num IN VARCHAR2,
                                        orchestrate_fwsts IN NUMBER,
                                        orchestrate_tellerid IN VARCHAR2,
                                        orchestrate_fwtime IN DATE,
                                        orchestrate_receiving_time IN DATE,
                                        orchestrate_sending_time IN DATE,
                                        orchestrate_trans_ref IN VARCHAR2,
                                        orchestrate_f07 IN VARCHAR2,
                                        orchestrate_f19 IN VARCHAR2,
                                        orchestrate_f21 IN VARCHAR2,
                                        orchestrate_f22 IN VARCHAR2,
                                        orchestrate_transdate IN NUMBER,
                                        orchestrate_print_sts IN NUMBER,
                                        orchestrate_msg_src IN NUMBER,
                                        orchestrate_product_type IN VARCHAR2,
                                        orchestrate_sibs_tellerid IN VARCHAR2)
    IS
     l_err_desc   VARCHAR2 (250);
    BEGIN
            IF dm_operation_type <> 'D'
                THEN
                   MERGE INTO sync_cdc_ibps_msg_content c
                     USING (SELECT orchestrate_msg_id AS msg_id,
                                   orchestrate_query_id AS query_id,
                                   orchestrate_file_name AS file_name,
                                   orchestrate_msg_direction AS msg_direction,
                                   orchestrate_trans_code AS trans_code,
                                   orchestrate_gw_trans_num AS gw_trans_num,
                                   orchestrate_sibs_trans_num AS sibs_trans_num,
                                   orchestrate_trans_date AS trans_date,
                                   orchestrate_amount AS amount,
                                   orchestrate_ccycd AS ccycd,
                                   orchestrate_status AS status,
                                   orchestrate_err_code AS err_code,
                                   orchestrate_trans_description AS trans_description,
                                   orchestrate_department AS department,
                                   orchestrate_content AS content,
                                   orchestrate_source_branch AS source_branch,
                                   orchestrate_tad AS tad,
                                   orchestrate_pre_tad AS pre_tad,
                                   orchestrate_rm_number AS rm_number,
                                   orchestrate_pretran_code AS pretran_code,
                                   orchestrate_pretrans_num AS pretrans_num,
                                   orchestrate_fwsts AS fwsts,
                                   orchestrate_tellerid AS tellerid,
                                   orchestrate_fwtime AS fwtime,
                                   orchestrate_receiving_time AS receiving_time,
                                   orchestrate_sending_time AS sending_time,
                                   orchestrate_trans_ref AS trans_ref,
                                   orchestrate_f07 AS f07,
                                   orchestrate_f19 AS f19,
                                   orchestrate_f21 AS f21,
                                   orchestrate_f22 AS f22,
                                   orchestrate_transdate AS transdate,
                                   orchestrate_print_sts AS print_sts,
                                   orchestrate_msg_src AS msg_src,
                                   orchestrate_product_type AS product_type,
                                   orchestrate_sibs_tellerid AS sibs_tellerid,
                                   SUBSTR (gw_pk_lib.get_ibps_field (orchestrate_content, '028'),
                                           1,
                                           200)
                                       AS send_name,
                                   REPLACE (
                                       REPLACE (
                                           REPLACE (
                                               gw_pk_lib.get_ibps_field (orchestrate_content,
                                                                         '030'),
                                               '.',
                                               ''),
                                           ' ',
                                           ''),
                                       '-',
                                       '')
                                       AS send_account,
                                   SUBSTR (gw_pk_lib.get_ibps_field (orchestrate_content, '031'),
                                           1,
                                           200)
                                       AS recieve_name,
                                   REPLACE (
                                       REPLACE (
                                           REPLACE (
                                               gw_pk_lib.get_ibps_field (orchestrate_content,
                                                                         '033'),
                                               '.',
                                               ''),
                                           ' ',
                                           ''),
                                       '-',
                                       '')
                                       AS receive_account
                              FROM DUAL) a
                        ON (c.msg_id = a.msg_id )--#20150526 Loctx remove  AND c.query_id = a.query_id (key la msg_id)
                    WHEN MATCHED
                    THEN
                        UPDATE SET c.file_name = a.file_name,
                                   c.msg_direction = a.msg_direction,
                                   c.trans_code = a.trans_code,
                                   c.gw_trans_num = a.gw_trans_num,
                                   c.sibs_trans_num = a.sibs_trans_num,
                                   c.trans_date = a.trans_date,
                                   c.amount = a.amount,
                                   c.ccycd = a.ccycd,
                                   c.status = a.status,
                                   c.err_code = a.err_code,
                                   c.trans_description = a.trans_description,
                                   c.department = a.department,
                                   c.content = a.content,
                                   c.source_branch = a.source_branch,
                                   c.tad = a.tad,
                                   c.pre_tad = a.pre_tad,
                                   c.rm_number = a.rm_number,
                                   c.pretran_code = a.pretran_code,
                                   c.pretrans_num = a.pretrans_num,
                                   c.fwsts = a.fwsts,
                                   c.tellerid = a.tellerid,
                                   c.fwtime = a.fwtime,
                                   c.receiving_time = a.receiving_time,
                                   c.sending_time = a.sending_time,
                                   c.trans_ref = a.trans_ref,
                                   c.f07 = a.f07,
                                   c.f19 = a.f19,
                                   c.f21 = a.f21,
                                   c.f22 = a.f22,
                                   c.transdate = a.transdate,
                                   c.print_sts = a.print_sts,
                                   c.msg_src = a.msg_src,
                                   c.product_type = a.product_type,
                                   c.sibs_tellerid = a.sibs_tellerid,
                                   c.send_name = a.send_name,
                                   c.send_account = a.send_account,
                                   c.recieve_name = a.recieve_name,
                                   c.receive_account = a.receive_account
                    WHEN NOT MATCHED
                    THEN
                        INSERT (c.msg_id,
                                c.query_id,
                                c.file_name,
                                c.msg_direction,
                                c.trans_code,
                                c.gw_trans_num,
                                c.sibs_trans_num,
                                c.trans_date,
                                c.amount,
                                c.ccycd,
                                c.status,
                                c.err_code,
                                c.trans_description,
                                c.department,
                                c.content,
                                c.source_branch,
                                c.tad,
                                c.pre_tad,
                                c.rm_number,
                                c.pretran_code,
                                c.pretrans_num,
                                c.fwsts,
                                c.tellerid,
                                c.fwtime,
                                c.receiving_time,
                                c.sending_time,
                                c.trans_ref,
                                c.f07,
                                c.f19,
                                c.f21,
                                c.f22,
                                c.transdate,
                                c.print_sts,
                                c.msg_src,
                                c.product_type,
                                c.sibs_tellerid,
                                c.send_name,
                                c.send_account,
                                c.recieve_name,
                                c.receive_account)
                        VALUES (a.msg_id,
                                a.query_id,
                                a.file_name,
                                a.msg_direction,
                                a.trans_code,
                                a.gw_trans_num,
                                a.sibs_trans_num,
                                a.trans_date,
                                a.amount,
                                a.ccycd,
                                a.status,
                                a.err_code,
                                a.trans_description,
                                a.department,
                                a.content,
                                a.source_branch,
                                a.tad,
                                a.pre_tad,
                                a.rm_number,
                                a.pretran_code,
                                a.pretrans_num,
                                a.fwsts,
                                a.tellerid,
                                a.fwtime,
                                a.receiving_time,
                                a.sending_time,
                                a.trans_ref,
                                a.f07,
                                a.f19,
                                a.f21,
                                a.f22,
                                a.transdate,
                                a.print_sts,
                                a.msg_src,
                                a.product_type,
                                a.sibs_tellerid,
                                a.send_name,
                                a.send_account,
                                a.recieve_name,
                                a.receive_account);

            END IF;
        COMMIT;
    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN --#20150527 LocTX add khong can raise exception
            ROLLBACK;
            l_err_desc := SUBSTR (SQLERRM, 1, 200);

            cspks_cdc_util.pr_log_sync_error (
                'GWSERVICE',
                'IBPS_MSG_CONTENT',
                'SYNC_CDC_IBPS_MSG_CONTENT',
                orchestrate_msg_id || ',' || orchestrate_query_id,
                dm_operation_type,
                l_err_desc);
            COMMIT;

        WHEN OTHERS
        THEN
            ROLLBACK;
            l_err_desc := SUBSTR (SQLERRM, 1, 200);

            cspks_cdc_util.pr_log_sync_error (
                'GWSERVICE',
                'IBPS_MSG_CONTENT',
                'SYNC_CDC_IBPS_MSG_CONTENT',
                orchestrate_msg_id || ',' || orchestrate_query_id,
                dm_operation_type,
                l_err_desc);
            COMMIT;
            RAISE;
    END;

    PROCEDURE pr_swift_msg_content_sync(dm_operation_type IN CHAR,
                                        orchestrate_msg_id IN NUMBER,
                                        orchestrate_query_id IN NUMBER,
                                        orchestrate_msg_type IN VARCHAR2,
                                        orchestrate_msg_direction IN VARCHAR2,
                                        orchestrate_branch_a IN VARCHAR2,
                                        orchestrate_branch_b IN VARCHAR2,
                                        orchestrate_trans_date IN DATE,
                                        orchestrate_value_date IN DATE,
                                        orchestrate_f20 IN VARCHAR2,
                                        orchestrate_f21 IN VARCHAR2,
                                        orchestrate_amount IN  NUMBER,
                                        orchestrate_ccycd IN  CHAR,
                                        orchestrate_foreign_bank IN VARCHAR2,
                                        orchestrate_foreign_bank_name IN VARCHAR2,
                                        orchestrate_priority IN NUMBER,
                                        orchestrate_deliver_type IN VARCHAR2,
                                        orchestrate_content IN CLOB,
                                        orchestrate_department IN VARCHAR2,
                                        orchestrate_auto IN CHAR,
                                        orchestrate_status IN NUMBER,
                                        orchestrate_swmsts IN VARCHAR2,
                                        orchestrate_err_code IN NUMBER,
                                        orchestrate_receiving_time IN DATE,
                                        orchestrate_sending_time IN DATE,
                                        orchestrate_session_no IN VARCHAR2,
                                        orchestrate_osn IN VARCHAR2,
                                        orchestrate_trans_no IN VARCHAR2,
                                        orchestrate_msg_no IN CHAR,
                                        orchestrate_seq_no IN CHAR,
                                        orchestrate_teller_id IN VARCHAR2,
                                        orchestrate_officer_id IN VARCHAR2,
                                        orchestrate_file_name IN VARCHAR2,
                                        orchestrate_pre_processsts IN VARCHAR2,
                                        orchestrate_rm_number IN VARCHAR2,
                                        orchestrate_statement_id IN NUMBER,
                                        orchestrate_isn IN VARCHAR2,
                                        orchestrate_nak_content IN NVARCHAR2,
                                        orchestrate_pre_branch IN VARCHAR2,
                                        orchestrate_pre_dept IN VARCHAR2,
                                        orchestrate_msg_src IN NUMBER,
                                        orchestrate_bic_receiver IN VARCHAR2,
                                        orchestrate_processsts IN VARCHAR2,
                                        orchestrate_transdate IN NUMBER,
                                        orchestrate_resend_num IN NUMBER,
                                    --    orchestrate_content_origin IN NVARCHAR2,
                                        orchestrate_print_sts IN NUMBER )
    IS
         l_err_desc   NVARCHAR2 (32000);
         l_bank_name NVARCHAR2 (32000);

         l_send_name NVARCHAR2(32000);
         l_receive_name NVARCHAR2(32000);

         l_send_account NVARCHAR2(32000);
         l_receive_account NVARCHAR2(32000);
         l_check_content NVARCHAR2(32000);
         l_remark NVARCHAR2(32000);

    BEGIN

    IF dm_operation_type <> 'D'
        THEN
        /*
            IF orchestrate_msg_type = 'MT202' THEN

                SELECT
                    NVL( GW_PK_LIB.GET_SWIFT_Field(DBMS_LOB.SUBSTR(content, 3900, 1), '52D', 2, 1, MSG_TYPE) , B.BANK_NAME) AS SEND_NAME,
                    f20 as SEND_ACCOUNT,
                    NVL(GW_PK_LIB.GET_SWIFT_Field(DBMS_LOB.SUBSTR(content, 3900, 1), '58D', 2, 1, MSG_TYPE) , C.BANK_NAME) AS RECIEVE_NAME ,
                    A.f21 as RECEIVE_ACCOUNT ,
                    substr(GW_PK_LIB.GET_SWIFT_Field(DBMS_LOB.SUBSTR(DBMS_LOB.SUBSTR(content, 3900, 1), 3900, 1), '72', 0, 1, MSG_TYPE), 1, 300) as REMARK

                INTO l_send_name, l_send_account, l_receive_name, l_receive_account, l_remark
                FROM
                (
                    SELECT orchestrate_content content,
                    orchestrate_f20 f20,
                    orchestrate_f21 f21,
                    orchestrate_msg_type msg_type
                    FROM DUAL
                )A
                LEFT JOIN SYNC_ETL_SWIFT_BANK_MAP B ON TRIM (B.SWIFT_BANK_CODE) =  GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '52A', 2, 1, MSG_TYPE)
                LEFT JOIN  SYNC_ETL_SWIFT_BANK_MAP C ON TRIM (C.SWIFT_BANK_CODE)  =  GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '58A', 2, 1, MSG_TYPE)
                ;
            ELSE
        */
                SELECT
                    CASE WHEN INSTRC(DBMS_LOB.SUBSTR(CONTENT, 3900, 1),'50F',1,1)>0 THEN GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '50F', 2, 1, MSG_TYPE) ELSE
                            GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '50K', 2, 1, MSG_TYPE) END AS SEND_NAME,
                    CASE WHEN INSTRC(DBMS_LOB.SUBSTR(CONTENT, 3900, 1),'50F',1,1)>0 THEN REPLACE(REPLACE(REPLACE(SUBSTR(GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '50F', 1, 1, MSG_TYPE),1, 50), '.', ''), ' ', ''), '-', '') ELSE
                            REPLACE(REPLACE(REPLACE(SUBSTR(GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '50K', 1, 1, MSG_TYPE),1, 50), '.', ''), ' ', ''), '-', '') END AS SEND_ACCOUNT,
                    GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '59', 2, 1, MSG_TYPE) AS RECIEVE_NAME ,
                    REPLACE(REPLACE(REPLACE(SUBSTR(GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), '59', 1, 1, MSG_TYPE),1, 50), '.', ''), ' ', ''), '-', '') AS RECEIVE_ACCOUNT ,
                    SUBSTR(GW_PK_LIB.GET_SWIFT_FIELD(DBMS_LOB.SUBSTR(DBMS_LOB.SUBSTR(CONTENT, 3900, 1), 3900, 1), '72', 0, 1, MSG_TYPE), 1, 300) AS REMARK
                INTO l_send_name, l_send_account, l_receive_name, l_receive_account, l_remark
                FROM
                (
                    SELECT orchestrate_content content,
                    orchestrate_f20 f20,
                    orchestrate_f21 f21,
                    orchestrate_msg_type msg_type
                    FROM DUAL
                )A

                ;
--            END IF;

            MERGE INTO sync_cdc_swift_msg_content c
             USING (SELECT orchestrate_msg_id AS msg_id,
                           orchestrate_query_id AS query_id,
                           orchestrate_msg_type AS msg_type,
                           orchestrate_msg_direction AS msg_direction,
                           TRIM(orchestrate_branch_a) AS branch_a,
                           TRIM(orchestrate_branch_b) AS branch_b,
                           orchestrate_trans_date AS trans_date,
                           orchestrate_value_date AS value_date,
                           orchestrate_f20 AS f20,
                           orchestrate_f21 AS f21,
                           orchestrate_amount AS amount,
                           orchestrate_ccycd AS ccycd,
                           orchestrate_foreign_bank AS foreign_bank,
                           orchestrate_foreign_bank_name AS foreign_bank_name,
                           orchestrate_priority AS priority,
                           orchestrate_deliver_type AS deliver_type,
                           orchestrate_content AS content,
                           orchestrate_department AS department,
                           orchestrate_auto AS auto,
                           orchestrate_status AS status,
                           orchestrate_swmsts AS swmsts,
                           orchestrate_err_code AS err_code,
                           orchestrate_receiving_time AS receiving_time,
                           orchestrate_sending_time AS sending_time,
                           orchestrate_session_no AS session_no,
                           orchestrate_osn AS osn,
                           orchestrate_trans_no AS trans_no,
                           orchestrate_msg_no AS msg_no,
                           orchestrate_seq_no AS seq_no,
                           orchestrate_teller_id AS teller_id,
                           orchestrate_officer_id AS officer_id,
                           orchestrate_file_name AS file_name,
                           orchestrate_pre_processsts AS pre_processsts,
                           orchestrate_rm_number AS rm_number,
                           orchestrate_statement_id AS statement_id,
                           orchestrate_isn AS isn,
                           orchestrate_nak_content AS nak_content,
                           orchestrate_pre_branch AS pre_branch,
                           orchestrate_pre_dept AS pre_dept,
                           orchestrate_msg_src AS msg_src,
                           orchestrate_bic_receiver AS bic_receiver,
                           orchestrate_processsts AS processsts,
                           orchestrate_transdate AS transdate,
                           orchestrate_resend_num AS resend_num,
                           orchestrate_print_sts AS print_sts,
                           l_send_name  AS send_name,
                           l_send_account AS send_account,
                           l_receive_name AS receive_name,
                           TRIM(l_receive_account) AS receive_account,
                           l_remark AS remark
                      FROM dual
                      ) a
                ON (c.msg_id = a.msg_id)--#20150526 Loctx remove  AND c.query_id = a.query_id (key la msg_id)
            WHEN MATCHED
            THEN
                UPDATE SET c.msg_type = a.msg_type,
                           c.msg_direction = a.msg_direction,
                           c.branch_a = a.branch_a,
                           c.branch_b = a.branch_b,
                           c.trans_date = a.trans_date,
                           c.value_date = a.value_date,
                           c.f20 = a.f20,
                           c.f21 = a.f21,
                           c.amount = a.amount,
                           c.ccycd = a.ccycd,
                           c.foreign_bank = a.foreign_bank,
                           c.foreign_bank_name = a.foreign_bank_name,
                           c.priority = a.priority,
                           c.deliver_type = a.deliver_type,
                           c.content = a.content,
                           c.department = a.department,
                           c.auto = a.auto,
                           c.status = a.status,
                           c.swmsts = a.swmsts,
                           c.err_code = a.err_code,
                           c.receiving_time = a.receiving_time,
                           c.sending_time = a.sending_time,
                           c.session_no = a.session_no,
                           c.osn = a.osn,
                           c.trans_no = a.trans_no,
                           c.msg_no = a.msg_no,
                           c.seq_no = a.seq_no,
                           c.teller_id = a.teller_id,
                           c.officer_id = a.officer_id,
                           c.file_name = a.file_name,
                           c.pre_processsts = a.pre_processsts,
                           c.rm_number = a.rm_number,
                           c.statement_id = a.statement_id,
                           c.isn = a.isn,
                           c.nak_content = a.nak_content,
                           c.pre_branch = a.pre_branch,
                           c.pre_dept = a.pre_dept,
                           c.msg_src = a.msg_src,
                           c.bic_receiver = a.bic_receiver,
                           c.processsts = a.processsts,
                           c.transdate = a.transdate,
                           c.resend_num = a.resend_num,
                           c.print_sts = a.print_sts,
                           c.send_name  = a.send_name,
                           c.send_account = a.send_account,
                           c.receive_name = a.receive_name,
                           c.receive_account = a.receive_account,
                           c.remark = a.remark
            WHEN NOT MATCHED
            THEN
                INSERT (c.msg_id,
                        c.query_id,
                        c.msg_type,
                        c.msg_direction,
                        c.branch_a,
                        c.branch_b,
                        c.trans_date,
                        c.value_date,
                        c.f20,
                        c.f21,
                        c.amount,
                        c.ccycd,
                        c.foreign_bank,
                        c.foreign_bank_name,
                        c.priority,
                        c.deliver_type,
                        c.content,
                        c.department,
                        c.auto,
                        c.status,
                        c.swmsts,
                        c.err_code,
                        c.receiving_time,
                        c.sending_time,
                        c.session_no,
                        c.osn,
                        c.trans_no,
                        c.msg_no,
                        c.seq_no,
                        c.teller_id,
                        c.officer_id,
                        c.file_name,
                        c.pre_processsts,
                        c.rm_number,
                        c.statement_id,
                        c.isn,
                        c.nak_content,
                        c.pre_branch,
                        c.pre_dept,
                        c.msg_src,
                        c.bic_receiver,
                        c.processsts,
                        c.transdate,
                        c.resend_num,
                        c.print_sts,
                        c.send_name,
                        c.send_account,
                        c.receive_name,
                        c.receive_account,
                        c.remark
                        )
                VALUES (a.msg_id,
                        a.query_id,
                        a.msg_type,
                        a.msg_direction,
                        a.branch_a,
                        a.branch_b,
                        a.trans_date,
                        a.value_date,
                        a.f20,
                        a.f21,
                        a.amount,
                        a.ccycd,
                        a.foreign_bank,
                        a.foreign_bank_name,
                        a.priority,
                        a.deliver_type,
                        a.content,
                        a.department,
                        a.auto,
                        a.status,
                        a.swmsts,
                        a.err_code,
                        a.receiving_time,
                        a.sending_time,
                        a.session_no,
                        a.osn,
                        a.trans_no,
                        a.msg_no,
                        a.seq_no,
                        a.teller_id,
                        a.officer_id,
                        a.file_name,
                        a.pre_processsts,
                        a.rm_number,
                        a.statement_id,
                        a.isn,
                        a.nak_content,
                        a.pre_branch,
                        a.pre_dept,
                        a.msg_src,
                        a.bic_receiver,
                        a.processsts,
                        a.transdate,
                        a.resend_num,
                        a.print_sts,
                        a.send_name,
                        a.send_account,
                        a.receive_name,
                        a.receive_account,
                        a.remark
                        );

            COMMIT;

        END IF;

    EXCEPTION
        WHEN DUP_VAL_ON_INDEX THEN --#20150527 LocTX add khong can raise exception
            ROLLBACK;
            l_err_desc := SUBSTR (SQLERRM, 1, 200);

            cspks_cdc_util.pr_log_sync_error (
                'GWSERVICE',
                'SWIFT_MSG_CONTENT',
                'SYNC_CDC_SWIFT_MSG_CONTENT',
                orchestrate_msg_id || ',' || orchestrate_query_id,
                dm_operation_type,
                l_err_desc);
            COMMIT;
        WHEN OTHERS
        THEN
            ROLLBACK;
            l_err_desc := SUBSTR (SQLERRM, 1, 200);

            cspks_cdc_util.pr_log_sync_error (
                'GWSERVICE',
                'SWIFT_MSG_CONTENT',
                'SYNC_CDC_SWIFT_MSG_CONTENT',
                orchestrate_msg_id || ',' || orchestrate_query_id,
                dm_operation_type,
                l_err_desc);
            COMMIT;
            RAISE;

    END;
END;

/
