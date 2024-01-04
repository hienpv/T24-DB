--------------------------------------------------------
--  DDL for Package Body PKG_MSB_BC_REG_API
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "IBS"."PKG_MSB_BC_REG_API" 
/* Formatted on 14-Sep-2017 14:21:39 (QP5 v5.126) */
IS
    FUNCTION FN_GET_PARAMETER_MSG(pv_msg IN CLOB, pv_parameter IN VARCHAR2, pv_path IN VARCHAR2) RETURN VARCHAR2
    IS
        l_parser  dbms_xmlparser.Parser;
        l_doc dbms_xmldom.DOMDocument;
        l_nodeList xmldom.domnodelist;
        l_node     xmldom.domnode;

        l_result VARCHAR2(500);
    BEGIN
        l_parser := dbms_xmlparser.newParser;
        dbms_xmlparser.parseClob(l_parser, pv_msg);
        l_doc := dbms_xmlparser.getDocument(l_parser);
        l_nodeList := xslprocessor.selectnodes(xmldom.makenode(l_doc),pv_path);

        FOR i IN 0 .. xmldom.getlength(l_nodeList) - 1 LOOP
          l_node := xmldom.item(l_nodeList, i);
          Dbms_Xslprocessor.Valueof(l_node , pv_parameter || '/text()' ,l_result);
        END LOOP;
        RETURN l_result;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line(SQLERRM);
            RETURN NULL;
    END;

    FUNCTION FN_GET_ATTRIBUTE_MSG(pv_msg IN CLOB, pv_attribute IN VARCHAR2, pv_path IN VARCHAR2) RETURN VARCHAR2
    IS
        l_parser  dbms_xmlparser.Parser;
        l_doc dbms_xmldom.DOMDocument;
        l_nodeList xmldom.domnodelist;
        l_node     xmldom.domnode;

        l_result VARCHAR2(500);
    BEGIN
        l_parser := dbms_xmlparser.newParser;
        dbms_xmlparser.parseClob(l_parser, pv_msg);
        l_doc := dbms_xmlparser.getDocument(l_parser);
        l_nodeList := xslprocessor.selectnodes(xmldom.makenode(l_doc),pv_path);

        FOR i IN 0 .. xmldom.getlength(l_nodeList) - 1 LOOP
          l_node         := xmldom.item(l_nodeList, i);
          l_result       := xmldom.getvalue(xmldom.getattributenode(xmldom.makeelement(l_node),
                                                                pv_attribute));
        END LOOP;

        return l_result;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line(SQLERRM);
        RETURN NULL;
    END;

    FUNCTION FN_SEND_CONTACT(ebankService IN EbankServiceRECORD, pv_fn_code IN VARCHAR2, p_user_id NUMBER) RETURN VARCHAR2
    IS
        l_params VARCHAR2(500);
        l_content CLOB;
        l_title VARCHAR2(100);
        l_template_type VARCHAR2(10);
        -- PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF pv_fn_code = SYS_FUNC_EMAIL AND ebankService.userInfo.email IS NOT NULL THEN
           l_template_type := 'E';
        ELSIF pv_fn_code = SYS_FUNC_SMS AND ebankService.userInfo.mobile IS NOT NULL THEN
           l_template_type := 'S';
        ELSE
           RETURN RESULT_EXCP;
        END IF;

        l_params := '%userName%=' || ebankService.userInfo.userName ||
                    SYS_DEFAULT_SPACE || '%password%=' || ebankService.userInfo.password ||
                    SYS_DEFAULT_SPACE || '%url%=' || SYS_DEFAULT_URL;
        -- '%userName%=khanh#%password%=aaaaaaa#%url%=http://www.msb.com.vn/'
        SELECT pkg_msb_util.multi_replace_clob(template_content, l_params), MESSAGE_TITLE INTO l_content, l_title
               FROM BK_MESSAGE_TEMPLATE WHERE
                    SYS_CODE = SYS_SYSTEM_CODE
                    AND TEMPLATE_TYPE = l_template_type
                    AND TEMPLATE_CODE = 'UR';

        IF l_content IS NULL THEN
           RETURN RESULT_EXCP;
        END IF;

        -- add message email or sms
        INSERT INTO BK_COMM_MESSAGE_LOG (MESSAGE_ID, SYS_CODE, USER_ID, USER_NAME, STATUS, CHANNEL_CODE, COMM_CHANNEL
        , MOBILE, EMAIL, CONTENT, OPERATE_TIME, TITLE, USER_GROUP, TC_CODE)
        VALUES
        ( seq_bk_log_id.nextval, SYS_SYSTEM_CODE, p_user_id, ebankService.userInfo.userName, OPER_STATUS_NEWR
        , SYS_CHANNEL_CODE, pv_fn_code, ebankService.userInfo.mobile, ebankService.userInfo.email
        , l_content, SYSDATE, l_title, NULL, NULL);

        RETURN RESULT_SUCC;
    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line(SQLERRM);
        RETURN RESULT_EXCP;
    END;

    PROCEDURE PRC_ANALYZE_MSG_XML (ebankService IN OUT EbankServiceRECORD, pv_errcode IN OUT VARCHAR2, pv_msg IN CLOB)
    IS
        l_parser  dbms_xmlparser.Parser;
        l_doc dbms_xmldom.DOMDocument;
        l_nodeList xmldom.domnodelist;
        l_node     xmldom.domnode;
        l_rootElement Dbms_Xmldom.Domelement;
        l_fncode varchar2(50);

        a_acct_no VARCHAR2(50);
        a_alias VARCHAR2(50);
        p_lst_account VARCHAR2(1000);
        p_time VARCHAR2(100);
    BEGIN
        l_parser := dbms_xmlparser.newParser;
        DBMS_OUTPUT.put_line (to_char(pv_msg));
        dbms_xmlparser.parseClob(l_parser, pv_msg);
        l_doc := dbms_xmlparser.getDocument(l_parser);
        l_rootElement := Dbms_Xmldom.Getdocumentelement(l_doc);
        l_fncode := lower(dbms_xmldom.getattribute(l_rootElement,'fncode'));
        ebankService.fnCode := l_fncode;

        l_nodeList := xslprocessor.selectnodes(xmldom.makenode(l_doc),'/EBankService');

        FOR i IN 0 .. xmldom.getlength(l_nodeList) - 1 LOOP
          l_node         := xmldom.item(l_nodeList, i);
          Dbms_Xslprocessor.Valueof(l_node ,'branchCode/text()' ,ebankService.branchCode);
          Dbms_Xslprocessor.Valueof(l_node ,'tellerId/text()' ,ebankService.tellerId);
          Dbms_Xslprocessor.Valueof(l_node ,'sentDate/text()' ,ebankService.sentDate);
          Dbms_Xslprocessor.Valueof(l_node ,'appReg/text()' ,ebankService.appReg);
        END LOOP;

        /* Read userInfo tag */
        l_nodeList := xslprocessor.selectnodes(xmldom.makenode(l_doc),'/EBankService/userInfo');
        FOR i IN 0 .. xmldom.getlength(l_nodeList) - 1 LOOP
          l_node         := xmldom.item(l_nodeList, i);
          Dbms_Xslprocessor.Valueof(l_node ,'cifno/text()' ,ebankService.userInfo.cifNo);
          Dbms_Xslprocessor.Valueof(l_node ,'email/text()' ,ebankService.userInfo.email);
          Dbms_Xslprocessor.Valueof(l_node ,'securityType/text()' ,ebankService.userInfo.securityType);
          Dbms_Xslprocessor.Valueof(l_node ,'userName/text()' ,ebankService.userInfo.userName);
          Dbms_Xslprocessor.Valueof(l_node ,'mobile/text()' ,ebankService.userInfo.mobile);
          Dbms_Xslprocessor.Valueof(l_node ,'openMbs/text()' ,ebankService.userInfo.openMbs);
          Dbms_Xslprocessor.Valueof(l_node ,'openIbs/text()' ,ebankService.userInfo.openIbs);
          Dbms_Xslprocessor.Valueof(l_node ,'groupId/text()' ,ebankService.userInfo.groupId);
          Dbms_Xslprocessor.Valueof(l_node ,'groupIdMbs/text()' ,ebankService.userInfo.groupIdMbs);
          Dbms_Xslprocessor.Valueof(l_node ,'passwordMD5/text()' ,ebankService.userInfo.passwordMD5);
          Dbms_Xslprocessor.Valueof(l_node ,'password/text()' ,ebankService.userInfo.password);

          Dbms_Xslprocessor.Valueof(l_node ,'gender/text()' ,ebankService.userInfo.eGender);
          Dbms_Xslprocessor.Valueof(l_node ,'branch_no/text()' ,ebankService.userInfo.eBranchNo);

          Dbms_Xslprocessor.Valueof(l_node ,'birth_date/text()' ,p_time);
          ebankService.userInfo.eBirthDate := TO_TIMESTAMP(p_time,'ddMMyyyy');

          Dbms_Xslprocessor.Valueof(l_node ,'individual/text()' ,ebankService.userInfo.eIndividual);
          Dbms_Xslprocessor.Valueof(l_node ,'cif_acct_name/text()' ,ebankService.userInfo.eCifAcctName);
          Dbms_Xslprocessor.Valueof(l_node ,'status/text()' ,ebankService.userInfo.eStatus);
          Dbms_Xslprocessor.Valueof(l_node ,'cert_type/text()' ,ebankService.userInfo.eCertType);
          Dbms_Xslprocessor.Valueof(l_node ,'cert_code/text()' ,ebankService.userInfo.eCertCode);
          Dbms_Xslprocessor.Valueof(l_node ,'contact_person/text()' ,ebankService.userInfo.eContactPerson);
          Dbms_Xslprocessor.Valueof(l_node ,'contact_addr/text()' ,ebankService.userInfo.eContactPerson);
          Dbms_Xslprocessor.Valueof(l_node ,'birth_place/text()' ,ebankService.userInfo.eBirthPlace);
          Dbms_Xslprocessor.Valueof(l_node ,'country/text()' ,ebankService.userInfo.eCountry);
          Dbms_Xslprocessor.Valueof(l_node ,'cif_no/text()' ,ebankService.userInfo.eCifNo);
          Dbms_Xslprocessor.Valueof(l_node ,'bank_no/text()' ,ebankService.userInfo.eBankNo);
          Dbms_Xslprocessor.Valueof(l_node ,'telephone/text()' ,ebankService.userInfo.eTelephone);

        END LOOP;

        /* Read account list tag */
        l_nodeList := xslprocessor.selectnodes(xmldom.makenode(l_doc),'/EBankService/acctNoList/RelatedAccountWS');
        FOR i IN 0 .. xmldom.getlength(l_nodeList) - 1 LOOP
          a_acct_no := '';
          a_alias   := '';
          p_lst_account := NVL(ebankService.acctNoList.strRelatedAccountWSs, '');
          l_node     := xmldom.item(l_nodeList, i);
          Dbms_Xslprocessor.Valueof(l_node ,'acctNo/text()' ,a_acct_no);
          Dbms_Xslprocessor.Valueof(l_node ,'alias/text()' ,a_alias);
          IF a_acct_no IS NOT NULL AND a_alias IS NOT NULL THEN
             p_lst_account := p_lst_account || 'acc' || a_acct_no || 'alias' || a_alias || '|';
             ebankService.acctNoList.relatedAccountWSs(i).acctNo := a_acct_no;
             ebankService.acctNoList.relatedAccountWSs(i).alias := a_alias;
             ebankService.acctNoList.strRelatedAccountWSs := p_lst_account;
          END IF;
          -- process RelatedAccountWS in here
        END LOOP;

        pv_errcode := '0';

        dbms_xmlparser.freeParser(l_parser);
        dbms_xmldom.freeDocument(l_doc);
    EXCEPTION
        WHEN OTHERS
        THEN
            dbms_xmlparser.freeParser(l_parser);
            dbms_xmldom.freeDocument(l_doc);
            DBMS_OUTPUT.put_line(SQLERRM);
            pv_errcode := RESULT_EXCP;
            ebankService := NULL;
    END;

    PROCEDURE PRC_RESET_IB_ACCTNO (pv_errcode IN OUT VARCHAR2, ebankService IN EbankServiceRECORD)
    IS
    -- cif ton tai, khop email, va trang thai la user active thi reset pass
        p_user_id NUMBER DEFAULT 0;
    BEGIN
--        -- Check exists cif_no in ib return reset pwd
--        select USER_ID INTO p_user_id from BC_USER_INFO
--               where CIF_NO = ebankService.userInfo.cifNo
--               and EMAIL = ebankService.userInfo.email
--               and status = OPER_STATUS_ACTV;
--
--        IF p_user_id <= 0 THEN
--            pv_errcode := RESULT_INVALID_DATA;
--            return;
--        END IF;
--
--        UPDATE BC_USER_INFO SET LOGIN_PWD = ebankService.userInfo.passwordMD5, UPDATE_TIME = sysdate,
--               LASTTIME_PWD_CHANGED = sysdate,
--               update_by = p_user_id
--        WHERE USER_ID = p_user_id;

--        pv_errcode := fn_send_contact(ebankService, SYS_FUNC_EMAIL, p_user_id);
    null;
    EXCEPTION
        WHEN OTHERS
        THEN
        pv_errcode := RESULT_EXCP;
    END;

    PROCEDURE PRC_VALID_IB_ACCTNO (pv_errcode IN OUT VARCHAR2, ebankService IN EbankServiceRECORD)
    IS
         p_exists_user NUMBER;
    BEGIN
        -- check null
        IF ebankService.branchCode IS NULL OR ebankService.tellerId IS NULL OR ebankService.sentDate IS NULL OR ebankService.appReg IS NULL THEN
          pv_errcode := RESULT_INVALID_DATA;
          return;
        END IF;

        -- check null user info
        IF ebankService.userInfo.cifNo IS NULL OR ebankService.userInfo.email IS NULL OR ebankService.userInfo.eGender IS NULL OR ebankService.userInfo.securityType IS NULL
          OR ebankService.userInfo.userName IS NULL OR ebankService.userInfo.passwordMD5 IS NULL
          OR ebankService.userInfo.password IS NULL THEN
           pv_errcode := RESULT_INVALID_DATA;
           return;
        END IF;

        IF ebankService.userInfo.securityType != SECURITY_TYPE_SM AND ebankService.userInfo.securityType != SECURITY_TYPE_TK THEN
           pv_errcode := RESULT_INVALID_DATA;
           return;
        END IF;

        -- when register in sms then mobile not null
        IF ebankService.userInfo.securityType = SECURITY_TYPE_SM AND ebankService.userInfo.mobile IS NULL THEN
           pv_errcode := RESULT_INVALID_DATA;
           return;
        END IF;

        IF ebankService.userInfo.securityType = SECURITY_TYPE_TK AND ebankService.tokenNo IS NULL THEN
           pv_errcode := RESULT_INVALID_DATA;
           return;
        END IF;

        -- check leng data const
        IF LENGTH(ebankService.userInfo.userName) > 50 OR LENGTH(ebankService.userInfo.securityType) != 2 THEN
          pv_errcode := RESULT_INVALID_DATA;
          return;
        END IF;

        -- check open mbs
        IF ebankService.userInfo.openMbs IS NULL OR (ebankService.userInfo.openMbs != 'Y' AND ebankService.userInfo.openMbs != 'y'AND ebankService.userInfo.openMbs != 'N'AND ebankService.userInfo.openMbs != 'n')THEN
           pv_errcode := RESULT_INVALID_DATA;
           return;
        END IF;

        -- check cif data
        IF ebankService.userInfo.cifNo != ebankService.userInfo.eCifNo THEN
           pv_errcode := RESULT_INVALID_DATA;
           return;
        END IF;

        -- Check exists cif_no in ib
--        select count(*) INTO p_exists_user from BC_USER_INFO
--               where CIF_NO = ebankService.userInfo.cifNo and status not in (OPER_STATUS_DLTD,OPER_STATUS_REGS);

        IF p_exists_user != 0 THEN
           pv_errcode := RESULT_CIF_EXISTS;
           return;
        END IF;

        -- Check exists user_name in ib
--        select count(*) INTO p_exists_user from BC_USER_INFO
--               where upper(USER_NAME) = upper(ebankService.userInfo.userName) and status not in (OPER_STATUS_DLTD,OPER_STATUS_REGS);

        IF p_exists_user != 0 THEN
           pv_errcode := RESULT_CIF_EXISTS;
           return;
        END IF;

        -- Check exists cif_no in ib return reset pwd
--        select count(*) INTO p_exists_user from BC_USER_INFO
--               where CIF_NO = ebankService.userInfo.cifNo
--               and upper(EMAIL) = upper(ebankService.userInfo.email)
--               and status = OPER_STATUS_ACTV;

        IF p_exists_user != 0 THEN
           pv_errcode := RESULT_CIF_EXISTS;
           return;
        END IF;

    EXCEPTION
        WHEN OTHERS
        THEN
            DBMS_OUTPUT.put_line(SQLERRM);
            pv_errcode := RESULT_EXCP;

    END;

    PROCEDURE PRC_REG_IB_ACCTNO (pv_errcode IN OUT VARCHAR2, ebankService IN EbankServiceRECORD)
    IS
    BEGIN

        PRC_VALID_IB_ACCTNO(pv_errcode, ebankService);
        IF pv_errcode != RESULT_SUCC THEN
           return;
        END IF;

        PRC_INSERT_NEW_USER(pv_errcode, ebankService);
        IF pv_errcode != RESULT_SUCC THEN
           return;
        END IF;
    EXCEPTION
        WHEN OTHERS
        THEN
        pv_errcode := RESULT_EXCP;
    END;

    PROCEDURE PRC_INSERT_NEW_USER (pv_errcode IN OUT VARCHAR2, ebankService IN EbankServiceRECORD)
    IS
       p_status VARCHAR2(50);
       p_log_status VARCHAR2(50);
       p_user_id NUMBER;
       p_approved_date DATE;

       p_list_related_acct ListRelatedAccountWS;
       p_parameter VARCHAR2(50);
       
       l_count NUMBER;
    BEGIN

        -- p_list_related_acct := ebankService.acctNoList.relatedAccountWSs;
        --pv_errcode := RESULT_SUCC;
        --return;

        IF ebankService.appReg = TSUNAMI_APP_PR OR ebankService.appReg = MPAYROLL_APP_PR
           OR ebankService.appReg = SMS_APP_PR OR ebankService.appReg = STARFISH_APP_PR
           OR ebankService.appReg = CCS_APP_PR THEN
           p_status := OPER_STATUS_ACTV;
           p_log_status := STATUS_APPROVED_REGISTER_LOG;
           p_approved_date := SYSDATE;
        ELSE
           p_status := OPER_STATUS_NEWR;
           p_log_status := STATUS_PENDING_REGISTER_LOG;
           p_approved_date := NULL;
        END IF;

        SELECT SEQ_USER_ID.nextval INTO p_user_id from dual;

        IF p_user_id IS NULL THEN
           pv_errcode := RESULT_EXCP;
           return;
        END IF;

        -- insert new user
--        INSERT INTO BC_USER_INFO (USER_ID, USER_NAME, NICK, GENDER, GROUP_ID, LOGIN_PWD, TRADE_PWD,
--        SECURITY_TYPE, SIGN_ORG, CERT_TYPE, CERT_NAME, CERT_CODE, CERT_ISSUED_DATE, CERT_ISSUED_PLACE,
--        TELEPHONE, MOBILE, FAX, ADDRESS, POSTAL_CODE, EMAIL, OPEN_ACCT_STMT, ACCT_STMT_FRQ,
--        ACCT_STMT_METHOD, OPEN_MOBILE, SERVICE_TYPE, RECEIVE_SMS_ADV, REMARK, CREATE_BY,
--        CREATE_BY_MNG, CREATE_TIME, UPDATE_BY, UPDATE_BY_MNG, UPDATE_TIME, STATUS, FREEZED_START_TIME,
--        FREEZED_END_TIME, LOGIN_COUNT, IS_ONLINE, IS_PWD_CHANGED, CIF_NO, CIF_ACCT_NAME, OPEN_IBS,
--        OPEN_MBS, OPEN_SMS, GROUP_ID_MBS, GROUP_ID_SMS, MOBILE_MBS, MOBILE_SMS, REGION_CODE, BRAND_CODE)
--        VALUES (p_user_id, ebankService.userInfo.userName, ebankService.userInfo.userName,
--        ebankService.userInfo.eGender, ebankService.userInfo.groupId, ebankService.userInfo.passwordMD5,
--        NULL, ebankService.userInfo.securityType, NULL, ebankService.userInfo.eCertType, NULL,
--        ebankService.userInfo.eCertCode, NULL, NULL, ebankService.userInfo.eTelephone,
--        ebankService.userInfo.mobile, NULL, ebankService.userInfo.eAddr, NULL, ebankService.userInfo.email,
--        NULL, NULL, NULL, NULL, NULL, NULL, ebankService.appReg, -1, NULL, SYSDATE, NULL, NULL, NULL,
--        p_status, NULL, NULL, 0, NULL, NULL, ebankService.userInfo.cifNo, ebankService.userInfo.eCifAcctName,
--        ebankService.userInfo.openIbs, ebankService.userInfo.openMbs, NULL, ebankService.userInfo.groupIdMbs,
--        NULL, NULL, NULL, NULL, ebankService.branchCode);
        -- insert log register user

        INSERT INTO BK_REGISTER_LOG (LOG_ID, PR_CIFNO, PR_EMAIL, PR_GENDER, PR_GROUPID, PR_MOBILE,
        PR_OPENMBS, PR_SECURIRY_TYPE, PR_USER_NAME, PR_TOKEN_NO, PR_TELLER_ID, PR_SENT_TIME, PR_ACCOUNT_LIST,
        PR_APP_REG, PR_BRAND_CODE, STATUS, LOG_COMMENT, CREATE_ID, APPROVE_ID, CREATE_DATE,
        APPROVE_DATE, OLD_CONTENT, NEW_CONTENT, APP_LOG_ID,USER_ID,CHANGE_TYPE)
        VALUES (SEQ_REGISTER_LOG.nextval, ebankService.userInfo.cifNo, ebankService.userInfo.email
        , ebankService.userInfo.eGender, ebankService.userInfo.groupId, ebankService.userInfo.mobile
        , ebankService.userInfo.openMbs, ebankService.userInfo.securityType, ebankService.userInfo.userName
        , ebankService.tokenNo, ebankService.tellerId, ebankService.sentDate, ebankService.acctNoList.strRelatedAccountWSs
        , ebankService.appReg, ebankService.branchCode, p_log_status, NULL
        , -1, -1, SYSDATE, p_approved_date, NULL, NULL, NULL, p_user_id, LOG_REGISTER_STATUS_NEWR);

        -- if new user token
--        IF ebankService.userInfo.securityType = SECURITY_TYPE_TK THEN
--           INSERT INTO BC_DIGITAL_TOKEN (TOKEN_NO, USER_ID, VALIDATED_TIME, EXPIRED_TIME, STATUS, BUY_TIME
--           , FREEZED_TYPE, FREEZED_START_TIME, FREEZED_END_TIME, AUTHOR_CODE, BUY_FEE, RSA_USER_ID)
--           VALUES
--           ( ebankService.tokenNo, p_user_id, NULL, NULL,
--           OPER_STATUS_ACTV, NULL, NULL, NULL,
--           NULL, NULL, 0, NULL );
--        END IF;

        p_list_related_acct := ebankService.acctNoList.relatedAccountWSs;
--        FOR i IN 0 .. p_list_related_acct.count - 1 LOOP
--            INSERT INTO BC_RELATED_ACCOUNT ( RELATION_ID, USER_ID, ACCT_NO, ACCT_TYPE, SUB_ACCT_TYPE
--            , IS_MASTER, ALIAS, STATUS, CREATE_BY, CREATE_TIME, UPDATE_BY)
--            VALUES
--            ( SEQ_RELATION_ID.nextval, p_user_id, p_list_related_acct(i).acctNo, NULL, NULL
--            , 'N', p_list_related_acct(i).alias, p_status, -1, sysdate, NULL);
--        END LOOP;

        INSERT INTO BK_REGISTER_LOG (LOG_ID, PR_CIFNO, PR_EMAIL, PR_GENDER, PR_GROUPID, PR_MOBILE,
        PR_OPENMBS, PR_SECURIRY_TYPE, PR_USER_NAME, PR_TOKEN_NO, PR_TELLER_ID, PR_SENT_TIME, PR_ACCOUNT_LIST,
        PR_APP_REG, PR_BRAND_CODE, STATUS, LOG_COMMENT, CREATE_ID, APPROVE_ID, CREATE_DATE,
        APPROVE_DATE, OLD_CONTENT, NEW_CONTENT, APP_LOG_ID,USER_ID,CHANGE_TYPE)
        VALUES (SEQ_REGISTER_LOG.nextval, ebankService.userInfo.cifNo, ebankService.userInfo.email
        , ebankService.userInfo.eGender, ebankService.userInfo.groupId, ebankService.userInfo.mobile
        , ebankService.userInfo.openMbs, ebankService.userInfo.securityType, ebankService.userInfo.userName
        , ebankService.tokenNo, ebankService.tellerId, ebankService.sentDate, ebankService.acctNoList.strRelatedAccountWSs
        , ebankService.appReg, ebankService.branchCode, p_log_status, NULL
        , -1, -1, SYSDATE, p_approved_date, NULL, NULL, NULL, p_user_id, LOG_REGISTER_STATUS_LKTK);

        SELECT NAME INTO p_parameter FROM bk_sys_parameter
               where CHANNEL_CODE = 'IB' AND TYPE = 'DATE_TO_EXPIRED' AND CODE = 'DTF';

--        INSERT INTO BC_PASSWORD_EXPIRED (USER_ID ,LOGIN_PWD,EXPIRED_DATE)
--        VALUES
--        (p_user_id, ebankService.userInfo.passwordMD5, sysdate + TO_NUMBER(p_parameter));
        
        l_count := 0;
        select count(*) INTO l_count from BK_CIF where CIF_NO = ebankService.userInfo.cifNo;
        
        IF l_count = 0 THEN
          insert into BK_CIF (CIF_NO, CERT_TYPE, CERT_CODE, BANK_NO, ORG_NO, CIF_ACCT_NAME,
          BIRTH_DATE, BIRTH_PLACE, COUNTRY, INDIVIDUAL, TELEPHONE, MOBILE, ADDR, POSTAL_CODE, EMAIL,
          SYNC_HIST, ID)
          values (ebankService.userInfo.cifNo, ebankService.userInfo.eCertType, ebankService.userInfo.eCertCode
          , ebankService.userInfo.eBankNo, NULL, ebankService.userInfo.eCifAcctName, ebankService.userInfo.eBirthDate
          , ebankService.userInfo.eBirthPlace, ebankService.userInfo.eCountry, ebankService.userInfo.eIndividual
          , ebankService.userInfo.eTelephone, ebankService.userInfo.mobile, ebankService.userInfo.eAddr,
          NULL, ebankService.userInfo.email, 1, 0);        
        END IF;

        pv_errcode := fn_send_contact(ebankService, SYS_FUNC_EMAIL, p_user_id);

    EXCEPTION
        WHEN OTHERS
        THEN
        DBMS_OUTPUT.put_line(SQLERRM);
        pv_errcode := RESULT_EXCP;
    END;

    PROCEDURE PRC_MESSAGE_PROCESS(pv_errcode IN OUT VARCHAR2, pv_msg IN CLOB)
    IS
        ebankService EbankServiceRECORD;
    BEGIN
        PRC_ANALYZE_MSG_XML(ebankService, pv_errcode, pv_msg);

        IF pv_errcode != RESULT_SUCC THEN
           return;
        END IF;

        /*Route function*/
        IF ebankService.fnCode = FUNC_REG_IB_ACCOUNT then
           PRC_REG_IB_ACCTNO(pv_errcode, ebankService);
        ELSIF (ebankService.fnCode = FUNC_RESET_IB_ACCOUNT) then
           PRC_RESET_IB_ACCTNO(pv_errcode, ebankService);
        ELSIF (ebankService.fnCode = FUNC_VALID_IB_ACCOUNT) then
           PRC_VALID_IB_ACCTNO(pv_errcode, ebankService);
        ELSE
            pv_errcode := RESULT_API_NOT_FOUND; -- API not found
        END IF;

        /* end process procudure */
        IF pv_errcode = RESULT_SUCC THEN
           commit;
        ELSE
           rollback;
        END IF;

    EXCEPTION
        WHEN OTHERS
        THEN
        pv_errcode := RESULT_EXCP;
    END;

    PROCEDURE PRC_MESSAGE_PROCESS_TEST(pv_errcode IN OUT VARCHAR2, pv_msg IN VARCHAR2)
    IS
        ebankService EbankServiceRECORD;
    BEGIN
        PRC_ANALYZE_MSG_XML(ebankService, pv_errcode, pv_msg);

        IF pv_errcode != RESULT_SUCC THEN
           return;
        END IF;

        /*Route function*/
        IF ebankService.fnCode = FUNC_REG_IB_ACCOUNT then
           PRC_REG_IB_ACCTNO(pv_errcode, ebankService);
        ELSIF (ebankService.fnCode = FUNC_RESET_IB_ACCOUNT) then
           PRC_RESET_IB_ACCTNO(pv_errcode, ebankService);
        ELSIF (ebankService.fnCode = FUNC_VALID_IB_ACCOUNT) then
           PRC_VALID_IB_ACCTNO(pv_errcode, ebankService);
        ELSE
            pv_errcode := RESULT_API_NOT_FOUND; -- API not found
        END IF;

        /* end process procudure */
        IF pv_errcode = RESULT_SUCC THEN
           commit;
        ELSE
           rollback;
        END IF;

    EXCEPTION
        WHEN OTHERS
        THEN
        pv_errcode := RESULT_EXCP;
    END;
END;

/
