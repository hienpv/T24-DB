--------------------------------------------------------
--  DDL for Package CSPKG_GWSERVICE_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "IBS"."CSPKG_GWSERVICE_SYNC" 
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
                                          orchestrate_sibs_tellerid IN VARCHAR2);

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
                                        orchestrate_print_sts IN NUMBER );

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
                                          orchestrate_sibs_tellerid IN VARCHAR2);

END;

/
