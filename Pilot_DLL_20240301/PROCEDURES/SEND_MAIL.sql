--------------------------------------------------------
--  DDL for Procedure SEND_MAIL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "SEND_MAIL" (
    msg_from       VARCHAR2 := 'oracle',
    msg_to         VARCHAR2,
    msg_subject    VARCHAR2 := 'E-Mail message from your database',
    msg_text       VARCHAR2 := '')
IS
    c    UTL_TCP.connection;
    rc   INTEGER;
BEGIN
    c := UTL_TCP.open_connection ('127.0.0.1', 25); -- open the SMTP port 25 on local machine
    DBMS_OUTPUT.put_line (UTL_TCP.get_line (c, TRUE));
    rc := UTL_TCP.write_line (c, 'HELO localhost');
    DBMS_OUTPUT.put_line (UTL_TCP.get_line (c, TRUE));
    rc := UTL_TCP.write_line (c, 'MAIL FROM: ' || msg_from);
    DBMS_OUTPUT.put_line (UTL_TCP.get_line (c, TRUE));
    rc := UTL_TCP.write_line (c, 'RCPT TO: ' || msg_to);
    DBMS_OUTPUT.put_line (UTL_TCP.get_line (c, TRUE));
    rc := UTL_TCP.write_line (c, 'DATA');                -- Start message body
    DBMS_OUTPUT.put_line (UTL_TCP.get_line (c, TRUE));
    rc := UTL_TCP.write_line (c, 'Subject: ' || msg_subject);
    rc := UTL_TCP.write_line (c, '');
    rc := UTL_TCP.write_line (c, msg_text);
    rc := UTL_TCP.write_line (c, '.');                  -- End of message body
    DBMS_OUTPUT.put_line (UTL_TCP.get_line (c, TRUE));
    rc := UTL_TCP.write_line (c, 'QUIT');
    DBMS_OUTPUT.put_line (UTL_TCP.get_line (c, TRUE));
    UTL_TCP.close_connection (c);                      -- Close the connection
EXCEPTION
    WHEN OTHERS
    THEN
        raise_application_error (
            -20000,
            'Unable to send e-mail message from pl/sql because of: '
            || SQLERRM);
END;

/
