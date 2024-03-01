--------------------------------------------------------
--  DDL for Package W4PKG_DATA_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "W4PKG_DATA_SYNC" 
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
 **  Loctx      23-01-2015    Created
 ** (c) 2014 by Financial Software Solutions. JSC.
 ----------------------------------------------------------------------------------------------------*/

    PROCEDURE pr_client_sync(dm_operation_type                 IN CHAR,
                            orchestrate_amnd_state              IN VARCHAR2,
                            orchestrate_amnd_date               IN DATE,
                            orchestrate_amnd_officer            IN NUMBER,
                            orchestrate_amnd_prev               IN NUMBER,
                            orchestrate_id                      IN NUMBER,
                            orchestrate_f_i                     IN NUMBER,
                            orchestrate_branch                  IN VARCHAR2,
                            orchestrate_ccat                    IN VARCHAR2,
                            orchestrate_pcat                    IN VARCHAR2,
                            orchestrate_clt                     IN NUMBER,
                            orchestrate_service_group           IN VARCHAR2,
                            orchestrate_short_name              IN VARCHAR2,
                            orchestrate_title                   IN NUMBER,
                            orchestrate_first_nam               IN VARCHAR2,
                            orchestrate_father_s_nam            IN VARCHAR2,
                            orchestrate_last_nam                IN VARCHAR2,
                            orchestrate_birth_nam               IN VARCHAR2,
                            orchestrate_mother_s_nam            IN VARCHAR2,
                            orchestrate_birth_date              IN DATE,
                            orchestrate_birth_place             IN VARCHAR2,
                            orchestrate_citizenship             IN VARCHAR2,
                            orchestrate_company_nam             IN VARCHAR2,
                            orchestrate_trade_nam               IN VARCHAR2,
                            orchestrate_url                     IN VARCHAR2,
                            orchestrate_company_department      IN VARCHAR2,
                            orchestrate_profession              IN VARCHAR2,
                            orchestrate_enable_affiliation      IN VARCHAR2,
                            orchestrate_affiliated_with         IN NUMBER,
                            orchestrate_reg_number_type         IN VARCHAR2,
                            orchestrate_reg_number              IN VARCHAR2,
                            orchestrate_reg_details             IN VARCHAR2,
                            orchestrate_affiliation_type        IN VARCHAR2,
                            orchestrate_client_number           IN VARCHAR2,
                            orchestrate_itn                     IN VARCHAR2,
                            orchestrate_tax_position            IN VARCHAR2,
                            orchestrate_country                 IN VARCHAR2,
                            orchestrate_state                   IN VARCHAR2,
                            orchestrate_city                    IN VARCHAR2,
                            orchestrate_address_zip             IN VARCHAR2,
                            orchestrate_address_line_1          IN VARCHAR2,
                            orchestrate_address_line_2          IN VARCHAR2,
                            orchestrate_address_line_3          IN VARCHAR2,
                            orchestrate_address_line_4          IN VARCHAR2,
                            orchestrate_phone                   IN VARCHAR2,
                            orchestrate_phone_h                 IN VARCHAR2,
                            orchestrate_phone_m                 IN VARCHAR2,
                            orchestrate_fax                     IN VARCHAR2,
                            orchestrate_fax_h                   IN VARCHAR2,
                            orchestrate_e_mail                  IN VARCHAR2,
                            orchestrate_delivery_type           IN NUMBER,
                            orchestrate_date_expire             IN DATE,
                            orchestrate_date_open               IN DATE,
                            orchestrate_gender                  IN VARCHAR2,
                            orchestrate_language                IN NUMBER,
                            orchestrate_marital_status          IN NUMBER,
                            orchestrate_tr_title                IN NUMBER,
                            orchestrate_tr_first_nam            IN VARCHAR2,
                            orchestrate_tr_last_nam             IN VARCHAR2,
                            orchestrate_tr_company_nam          IN VARCHAR2,
                            orchestrate_add_date_01             IN DATE,
                            orchestrate_add_date_02             IN DATE,
                            orchestrate_add_info_01             IN VARCHAR2,
                            orchestrate_add_info_02             IN VARCHAR2,
                            orchestrate_add_info_03             IN VARCHAR2,
                            orchestrate_add_info_04             IN VARCHAR2,
                            orchestrate_is_ready                IN VARCHAR2,
                            orchestrate_before_id               IN NUMBER);

    PROCEDURE pr_acnt_contract_sync(dm_operation_type                IN CHAR,
                                    orchestrate_amnd_date            IN DATE,
                                    orchestrate_amnd_state           IN VARCHAR2,
                                    orchestrate_amnd_officer         IN NUMBER,
                                    orchestrate_amnd_prev            IN NUMBER,
                                    orchestrate_id                   IN NUMBER,
                                    orchestrate_pcat                 IN VARCHAR2,
                                    orchestrate_con_cat              IN VARCHAR2,
                                    orchestrate_terminal_category    IN VARCHAR2,
                                    orchestrate_ccat                 IN VARCHAR2,
                                    orchestrate_f_i                  IN NUMBER,
                                    orchestrate_branch               IN VARCHAR2,
                                    orchestrate_service_group        IN VARCHAR2,
                                    orchestrate_contract_number      IN VARCHAR2,
                                    orchestrate_contract_name        IN VARCHAR2,
                                    orchestrate_comment_text         IN VARCHAR2,
                                    orchestrate_base_relation        IN VARCHAR2,
                                    orchestrate_relation_tag         IN VARCHAR2,
                                    orchestrate_acnt_contract__id    IN NUMBER,
                                    orchestrate_contr_type           IN NUMBER,
                                    orchestrate_contr_subtype__id    IN NUMBER,
                                    orchestrate_serv_pack__id        IN NUMBER,
                                    orchestrate_old_pack             IN NUMBER,
                                    orchestrate_channel              IN VARCHAR2,
                                    orchestrate_acc_scheme__id       IN NUMBER,
                                    orchestrate_old_scheme           IN NUMBER,
                                    orchestrate_product              IN VARCHAR2,
                                    orchestrate_product_prev         IN VARCHAR2,
                                    orchestrate_parent_product       IN VARCHAR2,
                                    orchestrate_main_product         IN NUMBER,
                                    orchestrate_client__id           IN NUMBER,
                                    orchestrate_client_type          IN NUMBER,
                                    orchestrate_acnt_contract__oid   IN NUMBER,
                                    orchestrate_liab_category        IN VARCHAR2,
                                    orchestrate_liab_contract        IN NUMBER,
                                    orchestrate_liab_contract_prev   IN NUMBER,
                                    orchestrate_billing_contract     IN NUMBER,
                                    orchestrate_behavior_group       IN NUMBER,
                                    orchestrate_behavior_type        IN NUMBER,
                                    orchestrate_behavior_type_prev   IN NUMBER,
                                    orchestrate_check_available      IN VARCHAR2,
                                    orchestrate_check_usage          IN VARCHAR2,
                                    orchestrate_curr                 IN VARCHAR2,
                                    orchestrate_old_curr             IN VARCHAR2,
                                    orchestrate_auth_limit_amount    IN NUMBER,
                                    orchestrate_base_auth_limit      IN NUMBER,
                                    orchestrate_liab_balance         IN NUMBER,
                                    orchestrate_liab_blocked         IN NUMBER,
                                    orchestrate_own_balance          IN NUMBER,
                                    orchestrate_own_blocked          IN NUMBER,
                                    orchestrate_sub_balance          IN NUMBER,
                                    orchestrate_sub_blocked          IN NUMBER,
                                    orchestrate_total_blocked        IN NUMBER,
                                    orchestrate_total_balance        IN NUMBER,
                                    orchestrate_shared_balance       IN NUMBER,
                                    orchestrate_shared_blocked       IN NUMBER,
                                    orchestrate_amount_available     IN NUMBER,
                                    orchestrate_date_open            IN DATE,
                                    orchestrate_date_expire          IN DATE,
                                    orchestrate_last_billing_date    IN DATE,
                                    orchestrate_next_billing_date    IN DATE,
                                    orchestrate_last_scan            IN DATE,
                                    orchestrate_card_expire          IN VARCHAR2,
                                    orchestrate_production_status    IN VARCHAR2,
                                    orchestrate_rbs_member_id        IN VARCHAR2,
                                    orchestrate_rbs_number           IN VARCHAR2,
                                    orchestrate_report_type          IN VARCHAR2,
                                    orchestrate_max_pin_attempts     IN NUMBER,
                                    orchestrate_pin_attempts         IN NUMBER,
                                    orchestrate_chip_scheme          IN NUMBER,
                                    orchestrate_risk_scheme          IN NUMBER,
                                    orchestrate_risk_factor          IN NUMBER,
                                    orchestrate_risk_factor_prev     IN NUMBER,
                                    orchestrate_contr_status         IN NUMBER,
                                    orchestrate_merchant_id          IN VARCHAR2,
                                    orchestrate_tr_title             IN NUMBER,
                                    orchestrate_tr_company           IN VARCHAR2,
                                    orchestrate_tr_country           IN VARCHAR2,
                                    orchestrate_tr_first_nam         IN VARCHAR2,
                                    orchestrate_tr_last_nam          IN VARCHAR2,
                                    orchestrate_tr_sic               IN VARCHAR2,
                                    orchestrate_add_info_01          IN VARCHAR2,
                                    orchestrate_add_info_02          IN VARCHAR2,
                                    orchestrate_add_info_03          IN VARCHAR2,
                                    orchestrate_add_info_04          IN VARCHAR2,
                                    orchestrate_contract_level       IN VARCHAR2,
                                    orchestrate_ext_data             IN VARCHAR2,
                                    orchestrate_report_address       IN NUMBER,
                                    orchestrate_share_balance        IN VARCHAR2,
                                    orchestrate_is_multycurrency     IN VARCHAR2,
                                    orchestrate_enables_item         IN VARCHAR2,
                                    orchestrate_cycle_length         IN NUMBER,
                                    orchestrate_interval_type        IN VARCHAR2,
                                    orchestrate_status_category      IN VARCHAR2,
                                    orchestrate_limit_is_active      IN VARCHAR2,
                                    orchestrate_settlement_type      IN VARCHAR2,
                                    orchestrate_is_ready             IN VARCHAR2,
                                    orchestrate_before_id            IN NUMBER);

END;

/