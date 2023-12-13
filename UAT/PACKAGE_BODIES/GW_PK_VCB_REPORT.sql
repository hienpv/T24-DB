--------------------------------------------------------
--  DDL for Package Body GW_PK_VCB_REPORT
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."GW_PK_VCB_REPORT" IS
  /* Ði?n in VCB
     Creator: Bùi H?ng Phuong
  */


  FUNCTION SWIFT_RM_GETFIELD_IN(pSWiftFile   varchar2,
                                pclobContent clob,
                                m_MSG_TYPE   varchar2) return m_tblField_type IS
    v_ReturnContent      m_tblField_type;
    v_ReturnContent_temp m_tblField_type;
    -- iPosStart       integer := 0;
    iPosEnd Integer := 0;
    --iPosStart Integer := 0;
    ilen        Integer := 0;
    iMSGLen     Integer := 0;
    v_Compare   varchar2(6);
    v_check_Acc varchar2(50);
    --v_FieldNext     varchar2(6);
    isOptionalTag  boolean := false;
    k              integer := 0;
    nstartReplace  integer := 0;
    v_Fieldcontent varchar2(4000);
    v_char_1       varchar2(2);
    v_char         varchar2(2);
    v_char1        varchar2(2);
    v_char2        varchar2(2);
    v_char3        varchar2(2);
    v_char4        varchar2(200);
    l              integer := 0;
    --v_char5        varchar2(2);
    vtest varchar2(2000);
    --vtest1 varchar2(2000);
    -- vtest2 varchar2(20);
  BEGIN
    for i in 0 .. 20 loop
      v_ReturnContent(i) := '';
    end loop;
    k       := 0;
    iMSGLen := Length(pclobContent);
    ilen    := Length(trim(pSWiftFile));

    -- lay ra vi tri dau tien cua doan chua du lieu Thuoc swift file name (VD field 20)
    nstartReplace := Dbms_Lob.instr(pclobContent, ':' || pSWiftFile || ':');

    if (pSWiftFile = '20') then
      k := 0;
    end if;

    --if iPosStart > 0 Then
    for m in nstartReplace - 6 .. imsglen loop
      if Dbms_Lob.substr(pclobContent, 1, m) = ':' and
         Dbms_Lob.substr(pclobContent, 1, m + 1 + ilen) = ':' then

        v_Compare := Dbms_Lob.substr(pclobContent, 2, m + 1); -- substr(pSWiftFile, 1, 2);
        if (ilen = 3) And substr(pSWiftFile, 3, 1) = 'a' then
          v_Compare := v_Compare || 'a';
        else
          v_Compare := v_Compare || Dbms_Lob.substr(pclobContent, 1, m + 3);
        end if;
        If (substr(v_Compare, 1, ilen) = substr(pSWiftFile, 1, ilen)) then
          if (ilen = 3) And substr(pSWiftFile, 3, 1) = 'a' then
            v_ReturnContent(0) := ''; --Dbms_Lob.substr(pclobContent, 1, m + 3);
          end if;

          if (ilen = 3 AND substr(pSWiftFile, 3, 1) = 'a') then
            v_ReturnContent(0) := ''; --dbms_lob.substr(pclobContent, 1, m + 3);
          End if;

          k := 1;

          for i in m + ilen + 2 .. iMSGLen Loop

            v_char_1 := dbms_lob.substr(pclobContent, 1, i - 1);
            v_char   := dbms_lob.substr(pclobContent, 1, i);

            v_char1 := dbms_lob.substr(pclobContent, 1, i + 1);

            v_char2 := dbms_lob.substr(pclobContent, 1, i + 2);

            v_char3 := dbms_lob.substr(pclobContent, 1, i + 3);
            v_char4 := dbms_lob.substr(pclobContent, 1, i + 4);
            vtest   := v_char_1 || v_char || v_char1 || v_char2 || v_char3 ||
                       v_char4;

            if ((v_char = '-' AND v_char1 = '}') Or
               ((v_char_1 = chr(10) or (v_char_1 = chr(13))) And
               v_char = ':' And (v_char3 = ':' or v_char4 = ':'))) Then

              Exit;
            END IF;

            iPosEnd := i;
            v_char  := dbms_lob.substr(pclobContent, 1, i);
            IF v_char = chr(10) Then
              --Kiem tra
              if (isOptionalTag AND (k = 1) And
                 dbms_lob.substr(pclobContent, 1, i) <> '/') then
                -- Cai nay lam gi day
                v_ReturnContent(2) := v_ReturnContent(1);
                v_ReturnContent(1) := ' ';

                k := 3;
                l := i + 1;
              else
                if (k > 10) then
                  k := k;
                end if;
                k := k + 1;
                l := i + 1;
              End if;
              -- lay noi dung cua truong dien

            else

              IF dbms_lob.substr(pclobContent, 1, i) <> chr(13) Then
                --lay vi tri dau tien  cua doan du lieu tiep theo

                --iRow_pos := i + 1;

                v_Fieldcontent := dbms_lob.substr(pclobContent, 1, i);
                v_ReturnContent(k) := v_ReturnContent(k) || v_Fieldcontent;
                v_Fieldcontent := v_ReturnContent(k);

              end if;
            end if;

          End loop;

          Exit;
        End if;
      end if;

    end loop;

    if (substr(pSWiftFile, 1, 3) = '23E') then

      v_ReturnContent(2) := substr(v_ReturnContent(2), 1, 5) ||
                            substr(v_ReturnContent(1), 5);
      v_ReturnContent(1) := substr(v_ReturnContent(1), 1, 4);
    End if;

    if pSWiftFile = '32A' Then
      --vtest := v_ReturnContent(1);
      -- Tach truong 32A ra thanh 3 phan ngay, loai tien, so tien
      v_ReturnContent(2) := substr(v_ReturnContent(1), 7, 3);
      v_ReturnContent(3) := substr(v_ReturnContent(1), 10);
      v_ReturnContent(1) := substr(v_ReturnContent(1), 1, 6);
      -- lay loai tien
      -- lay ngay create date
      -- Remember that SWIFT date format is YYMMDD, while SIBS is DDMMYY
      begin

        if trim(v_ReturnContent(1)) is not null then
          v_Fieldcontent := to_char(to_date(v_ReturnContent(1), 'YYMMDD'),
                                    'YYMMDD');

        else
          v_Fieldcontent := null;
        end if;

      exception
        when others then
          v_Fieldcontent := null;
      End;

    End if;

    if (pSWiftFile = '32B') then

      v_ReturnContent(2) := substr(v_ReturnContent(1), 4);
      v_ReturnContent(1) := substr(v_ReturnContent(1), 1, 3);
    END IF;
    if (pSWiftFile = '33B') then
      v_ReturnContent(2) := substr(v_ReturnContent(1), 4);
      v_ReturnContent(1) := substr(v_ReturnContent(1), 1, 3);
    End if;

    if (pSWiftFile = '71F') then
      v_ReturnContent(2) := substr(v_ReturnContent(1), 4);
      v_ReturnContent(1) := substr(v_ReturnContent(1), 1, 3);
    End if;
    if (pSWiftFile = '71G') then
      v_ReturnContent(2) := substr(v_ReturnContent(1), 4);
      v_ReturnContent(1) := substr(v_ReturnContent(1), 1, 3);
    End if;

    --if m_Type_SWIFT_Content.Department = 'RM' then
    v_ReturnContent_temp := v_ReturnContent;
    ----------------------
    /*Phuong sua: 26052010
      Reason : chat field khong chuan
      Edit  : Comment all
    */
    /*if m_MSG_TYPE = 'MT103' then
      if substr(pSWiftFile, 1, 2) = '52' or substr(pSWiftFile, 1, 2) = '53' or
         substr(pSWiftFile, 1, 2) = '50' or substr(pSWiftFile, 1, 2) = '54' or
         substr(pSWiftFile, 1, 2) = '55' or substr(pSWiftFile, 1, 2) = '56' or
         substr(pSWiftFile, 1, 2) = '57' or substr(pSWiftFile, 1, 2) = '59' then
        v_check_Acc := v_ReturnContent(1);
      if substr(v_check_Acc, 1, 1) <> '/' then
          for i in 2 .. 10 loop
            v_check_Acc := v_ReturnContent(i);

            v_ReturnContent(i) := v_ReturnContent_temp(i - 1);
          end loop;
          v_ReturnContent(1) := '';
        end if;
      end if;
    end if;

    if m_MSG_TYPE = 'MT191' then
      if substr(pSWiftFile, 1, 2) = '52' or substr(pSWiftFile, 1, 2) = '57' then
        v_check_Acc := v_ReturnContent(1);
        if substr(v_check_Acc, 1, 1) <> '/' then
          for i in 2 .. 10 loop
            v_check_Acc := v_ReturnContent(i);
            v_ReturnContent(i) := v_ReturnContent_temp(i - 1);
          end loop;
          v_ReturnContent(1) := '';
        end if;

      end if;

    end if;

    if m_MSG_TYPE = 'MT202' then
      if substr(pSWiftFile, 1, 2) = '52' or substr(pSWiftFile, 1, 2) = '53' or
         substr(pSWiftFile, 1, 2) = '54' or substr(pSWiftFile, 1, 2) = '58' or
         substr(pSWiftFile, 1, 2) = '56' or substr(pSWiftFile, 1, 2) = '57' then
        v_check_Acc := v_ReturnContent(1);
        if substr(v_check_Acc, 1, 1) <> '/' then
          for i in 2 .. 10 loop
            v_check_Acc := v_ReturnContent(i);
            v_ReturnContent(i) := v_ReturnContent_temp(i - 1);
          end loop;
          v_ReturnContent(1) := '';
        end if;

      end if;

    end if;

    if m_MSG_TYPE = 'MT900' then
      if substr(pSWiftFile, 1, 2) = '52'

       then
        v_check_Acc := v_ReturnContent(1);
        if substr(v_check_Acc, 1, 1) <> '/' then
          for i in 2 .. 10 loop
            v_check_Acc := v_ReturnContent(i);
            v_ReturnContent(i) := v_ReturnContent_temp(i - 1);
          end loop;
          v_ReturnContent(1) := '';
        end if;

      end if;

    end if;
    if m_MSG_TYPE = 'MT910' then
      if substr(pSWiftFile, 1, 2) = '52' or substr(pSWiftFile, 1, 2) = '50' or
         substr(pSWiftFile, 1, 2) = '56'

       then
        v_check_Acc := v_ReturnContent(1);
        if substr(v_check_Acc, 1, 1) <> '/' then
          for i in 2 .. 10 loop
            v_check_Acc := v_ReturnContent(i);
            v_ReturnContent(i) := v_ReturnContent_temp(i - 1);
          end loop;
          v_ReturnContent(1) := '';
        end if;

      end if;

    end if;
    */
    ------------------------------
    -- end if;
    --
    Return v_ReturnContent;
  Exception
    when OTHERS THEN

      return v_ReturnContent;
  END SWIFT_RM_GETFIELD_IN;

  /**********************************************************************
  Nguoi tao:  QuanLD
  Muc dich: Lay noi dung cua cac truong trong dien
  Co tinh den cac truong hop cua cac truong dien dac biet phai xu lys rieng

    Ten ham:  SWIFT_RM_GETFIELD_IN()
  Tham so:  pSWiftFile varchar2, pclobContent clob
  Mo ta: -

  Ngay khoi tao:  13/06/2008
  Nguoi sua:
  Ngay sua:
  Mo ta

  **********************************************************************/

  FUNCTION SWIFT_RM_GETFIELD_IN(pSWiftFile   varchar2,
                                pclobContent clob,
                                m_MSG_TYPE   varchar2,
                                prownum      integer) return m_tblField_type IS
    v_ReturnContent      m_tblField_type;
    v_ReturnContent_temp m_tblField_type;
    -- iPosStart       integer := 0;
    iPosEnd Integer := 0;
    --iPosStart Integer := 0;
    ilen        Integer := 0;
    iMSGLen     Integer := 0;
    v_Compare   varchar2(6);
    v_check_Acc varchar2(50);
    --v_FieldNext     varchar2(6);
    isOptionalTag  boolean := false;
    k              integer := 0;
    nstartReplace  integer := 0;
    v_Fieldcontent varchar2(4000);
    v_char_1       varchar2(2);
    v_char         varchar2(2);
    v_char1        varchar2(2);
    v_char2        varchar2(2);
    v_char3        varchar2(2);
    v_char4        varchar2(200);
    l              integer := 0;
    --v_char5        varchar2(2);
    vtest varchar2(2000);
    --vtest1 varchar2(2000);
    -- vtest2 varchar2(20);
  BEGIN
    for i in 0 .. 30 loop
      v_ReturnContent(i) := '';
    end loop;
    k       := 0;
    iMSGLen := Length(pclobContent);
    ilen    := Length(trim(pSWiftFile));
    -- lay ra vi tri dau tien cua doan chua du lieu Thuoc swift file name (VD field 20)
    nstartReplace := Dbms_Lob.instr(pclobContent, ':' || pSWiftFile || ':');

    --if iPosStart > 0 Then
    for m in nstartReplace - 6 .. imsglen loop
      if Dbms_Lob.substr(pclobContent, 1, m) = ':' and
         Dbms_Lob.substr(pclobContent, 1, m + 1 + ilen) = ':' then

        v_Compare := Dbms_Lob.substr(pclobContent, 2, m + 1); -- substr(pSWiftFile, 1, 2);
        if (ilen = 3) And substr(pSWiftFile, 3, 1) = 'a' then
          v_Compare := v_Compare || 'a';
        else
          v_Compare := v_Compare || Dbms_Lob.substr(pclobContent, 1, m + 3);
        end if;
        If (substr(v_Compare, 1, ilen) = substr(pSWiftFile, 1, ilen)) then
          if (ilen = 3) And substr(pSWiftFile, 3, 1) = 'a' then
            v_ReturnContent(0) := Dbms_Lob.substr(pclobContent, 1, m + 3);
          end if;

          if (ilen = 3 AND substr(pSWiftFile, 3, 1) = 'a') then
            v_ReturnContent(0) := dbms_lob.substr(pclobContent, 1, m + 3);
          End if;

          k := 1;

          for i in m + ilen + 2 .. iMSGLen Loop

            v_char_1 := dbms_lob.substr(pclobContent, 1, i - 1);
            v_char   := dbms_lob.substr(pclobContent, 1, i);

            v_char1 := dbms_lob.substr(pclobContent, 1, i + 1);

            v_char2 := dbms_lob.substr(pclobContent, 1, i + 2);

            v_char3 := dbms_lob.substr(pclobContent, 1, i + 3);
            v_char4 := dbms_lob.substr(pclobContent, 1, i + 4);

            if ((v_char = '-' AND v_char1 = '}') Or
               ((v_char_1 = chr(10) or (v_char_1 = chr(13))) And
               v_char = ':' And (v_char3 = ':' or v_char4 = ':'))) Then

              Exit;
            END IF;

            iPosEnd := i;
            v_char  := dbms_lob.substr(pclobContent, 1, i);
            IF v_char = chr(10) Then
              --Kiem tra
              if (isOptionalTag AND (k = 1) And
                 dbms_lob.substr(pclobContent, 1, i) <> '/') then
                -- Cai nay lam gi day
                v_ReturnContent(2) := v_ReturnContent(1);
                v_ReturnContent(1) := ' ';

                k := 3;
                l := i + 1;
              else
                if (k > 10) then
                  k := k;
                end if;
                k := k + 1;
                l := i + 1;
              End if;
              -- lay noi dung cua truong dien

            else

              IF dbms_lob.substr(pclobContent, 1, i) <> chr(13) Then
                --lay vi tri dau tien  cua doan du lieu tiep theo

                --iRow_pos := i + 1;

                v_Fieldcontent := dbms_lob.substr(pclobContent, 1, i);
                v_ReturnContent(k) := v_ReturnContent(k) || v_Fieldcontent;
                v_Fieldcontent := v_ReturnContent(k);

              end if;
            end if;

          End loop;

          Exit;
        End if;
      end if;

    end loop;

    if pSWiftFile = '32A' Then
      v_ReturnContent(1) := Replace(v_ReturnContent(1), ',', '.');
    End if;

    if (pSWiftFile = '32B' or pSWiftFile = '33B' or pSWiftFile = '71F' or
       pSWiftFile = '71G') then
      v_ReturnContent(1) := Replace(v_ReturnContent(1), ',', '.');
    END IF;
    Return v_ReturnContent;
  Exception
    when OTHERS THEN

      return v_ReturnContent;
  END SWIFT_RM_GETFIELD_IN;

  ----------------------------------------
  -- Lay noi dung ca field dien
  ----------------------------------------

  FUNCTION SWIFT_RM_GETFIELD_IN(pFieldName varchar2, pCONTENT clob)
    return varchar2 IS
    v_returnContent varchar2(1000);
    vstrTemp        varchar2(1500);
    iposStart       integer;
  Begin
    -- lay toan bo row

    iposStart := dbms_lob.instr(pCONTENT,
                                ':' || UPPER(LTRIM(Trim(pFieldName), 'F')) || ':');

    if iposStart > 0 then

      vstrTemp := dbms_lob.substr(pCONTENT, 1000, iposStart + 4);
      if (substr(vstrTemp, 1, 1) = '/') or
         (substr(vstrTemp, 1, 1) = ':' and substr(vstrTemp, 2, 1) <> '/') then
        vstrTemp := substr(vstrTemp, 2, length(vstrTemp) - 1);
      elsif (substr(vstrTemp, 1, 1) = ':') and
            (substr(vstrTemp, 2, 1) = '/') then
        vstrTemp := substr(vstrTemp, 3, length(vstrTemp) - 2);
      elsif (substr(vstrTemp, 2, 1) = ':') and
            (substr(vstrTemp, 3, 1) = '/') then
        vstrTemp := substr(vstrTemp, 4, length(vstrTemp) - 3);
      end if;
      for i in 1 .. Length(vstrTemp) loop
        if substr(vstrTemp, i, 1) = ':' And
           (substr(vstrTemp, i + 4, 1) = ':' or
            substr(vstrTemp, i + 3, 1) = ':') AND
           (substr(vstrTemp, i - 1, 1) = chr(10) or
            substr(vstrTemp, i - 1, 1) = chr(13)) or
           substr(vstrTemp, i, 1) = '}' then
          v_returnContent := substr(vstrTemp, 1, i - 1);
          exit;
        end if;

      end loop;

    end if;
    Return v_returnContent;
  end SWIFT_RM_GETFIELD_IN;

  FUNCTION VCB_GET_FIELD(pCONTENT   VARCHAR2,
                         pFieldName VARCHAR2,
                         m_MSG_TYPE nvarchar2) RETURN VARCHAR2 IS

  BEGIN
    RETURN GW_PK_VCB_REPORT.VCB_GET_SWIFT_Field(pCONTENT,
                                                pFieldName,
                                                0,
                                                0,
                                                m_MSG_TYPE);

  END VCB_GET_FIELD;
  ---------------------
  FUNCTION VCB_GET_SWIFT_Field(pCOntent   clob,
                               pFiledCode varchar2,
                               pRownum    number,
                               pPartnum   number,
                               m_MSG_TYPE varchar2) Return Varchar2 IS
    v_Value varchar2(2000);

    v_ReturnContent m_tblField_type;

  BEGIN

    -- Lay noi dung dien theo tung rownum/partnumb
    if pRownum > 0 and pPartnum > 0 then
      v_ReturnContent := SWIFT_RM_GETFIELD_IN(pFiledCode,
                                              pCOntent,
                                              m_MSG_TYPE);
      v_Value         := GetFieldValue(pFiledCode,
                                       pRownum,
                                       pPartnum,
                                       v_ReturnContent);
    else
      -- lay ca 1 rownum cua field
      if pRownum > 0 and pPartnum = 0 then
        v_ReturnContent := SWIFT_RM_GETFIELD_IN(pFiledCode,
                                                pCOntent,
                                                m_MSG_TYPE);
        v_Value         := GetFieldValue(pFiledCode,
                                         pRownum,
                                         0,
                                         v_ReturnContent);

      else
        --lay ca noi dung 1 field
        v_Value := SWIFT_RM_GETFIELD_IN(pFiledCode, pCOntent);
      end if;

    end if;
    v_Value := Ltrim(v_Value, chr(13));
    v_Value := Ltrim(v_Value, chr(10));
    v_Value := Rtrim(v_Value, chr(13));

    if substr(pFiledCode, 3, 1) = 'a' then
      v_Value := '';
    end if;
    return Rtrim(Rtrim(v_Value, chr(13)), chr(10));
  Exception
    when others then
      Return '';
  END VCB_GET_SWIFT_Field;
  FUNCTION GetFieldValue(pSWFieldTag  varchar2,
                         piRowNum     integer,
                         piPartNum    Integer,
                         pFilecontent m_tblField_type) return Varchar2 IS

    v_Value varchar2(4000);
    ipos    integer := 0;
    ipos1   integer := 0;
    vtest   varchar2(4000);
    --Test1   varchar2(200) := '';
  BEGIN
    if piRowNum > pFilecontent.LAST then
      return ' ';
    end if;
    v_Value := '';

    if (not pFilecontent(0) is null) Then

      if (substr(pFilecontent(0), 1, 1) = substr(pSWFieldTag, 3, 1) or
         piRowNum = 0) Then
        v_Value := pFilecontent(piRowNum);
      else
        v_Value := '';
      End if;
    else
      v_Value := pFilecontent(piRowNum);
      if (substr(pSWFieldTag, 1, 2) = '72') Then
        -- neu dong chi co 1 phan partnum=1
        if (piPartNum = 1) Then
          if (substr(pFilecontent(piRowNum), 1, 1) = '/' AND
             substr(pFilecontent(piRowNum), 2, 1) <> '/') Then
            ipos1 := instr(substr(pFilecontent(piRowNum), 2, 9), '/');
            if ipos1 > 0 then
              v_Value := substr(pFilecontent(piRowNum), 2, ipos1 - 1);
            end if;

          else

            v_Value := '';
          End if;
        else
          -- Neu dong co 2 phan PartNum==2
          -- kiem tra xem co ky tu phan biet tung phan cua dien hay khong
          if (substr(pFilecontent(piRowNum), 1, 1) = '/' AND
             substr(pFilecontent(piRowNum), 2, 1) <> '/') then
            ipos := instr(substr(pFilecontent(piRowNum), 3), '/');

            --ipos    := instr(substr(pFilecontent(piRowNum), j + 1), '/');
            v_Value := substr(pFilecontent(piRowNum), ipos + 1);
            ipos    := instr(v_Value, '/');
            v_Value := substr(v_Value, ipos + 1);
          else
            v_Value := pFilecontent(piRowNum);

          end if;
        End if;
      end if;
    End if;
    if (substr(pSWFieldTag, 1, 2) = '72') Then
      if substr(v_Value, 1, 1) <> '/' then
        v_Value := '/' || v_Value;
      end if;
    else
      v_Value := LTrim(v_Value, '/');
    end if;

    v_Value := LTrim(v_Value, '/');

    if substr(pSWFieldTag, 3, 1) = 'a' then
      v_Value := '';
    end if;
    Return Replace(Replace(v_Value, chr(13)), chr(10));
  Exception
    when OTHERS THEN
      v_Value := '';
      return v_Value;
  END;
  ---------------------------------------------

END;

/
