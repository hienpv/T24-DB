--------------------------------------------------------
--  DDL for Package Body PK_UPDATE_DATA
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "T24IBS"."PK_UPDATE_DATA" AS

  PROCEDURE SYNC_DATA_SWIFT_INFO AS
  BEGIN
    -- Kiem tra data tren co tren CORE ma khong co trong BK_SWIFT_INFO thi insert vao
    FOR data_rec IN (
      select trim(a.SWBCOD) SWBCOD, trim(max(a.SWBNAM)) SWBNAM, trim(max(a.SWBAD1)) SWBAD1, trim(max(a.SWCONT)) SWCONT
      from STG.SI_PAR_SWBICD@STAGING_PRO a
      left join BK_SWIFT_INFO b on trim(a.SWBCOD) = b.SWIFT_CODE
      where b.SWIFT_CODE is null
      group by trim(a.SWBCOD)
    )
    LOOP
      INSERT INTO BK_SWIFT_INFO (SWIFT_CODE,SWIFT_NAME,SWIFT_ADD,SWIFT_REGION)
      VALUES(data_rec.SWBCOD, data_rec.SWBNAM, data_rec.SWBAD1, data_rec.SWCONT);
    END LOOP;
    commit;
    -- Co data trong bang BK_SWIFT_INFO ma khong co tren CORE thi xoa di
    delete from BK_SWIFT_INFO where SWIFT_CODE IN (
      select a.SWIFT_CODE
      from BK_SWIFT_INFO a
      left join STG.SI_PAR_SWBICD@STAGING_PRO b on trim(b.SWBCOD) = a.SWIFT_CODE
      where b.SWBCOD is null
    );
    commit;
  END SYNC_DATA_SWIFT_INFO;

 
END PK_UPDATE_DATA;

/
