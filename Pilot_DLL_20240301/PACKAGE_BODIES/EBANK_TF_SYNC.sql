--------------------------------------------------------
--  DDL for Package Body EBANK_TF_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "EBANK_TF_SYNC" IS
  v_start_date DATE;

  PROCEDURE proc_tf_info_sync IS
  BEGIN
    v_start_date := SYSDATE;

    INSERT INTO sync_tfmast
      SELECT
      /*+ ALL_ROWS */
       t1.tmocif,
       t1.tmprdc,
       t1.tmtref,
       t1.tmucy3,
       t1.tmrvcb,
       t1.tmoram,
       t1.tmtenr,
       t1.tmsrbr,
       t1.tmpdtp,
       t1.tmglgc,
       t1.tmappn,
       t1.tmfacs,
       t1.tmblam,
       t1.tmbosa,
       t1.tmbosl,
       t1.tmpmoa,
       t1.tmstat,
       t1.tmissd,
       t1.tmissj,
       t1.tmcexj,
       t1.tmexpd,
       t1.tmexpj,
       t1.TMOEXJ
      FROM   RAWSTAGEUAT.SI_DAT_TFMAST@RAWSTAGE_PRO_CORE t1;

    MERGE INTO bk_tf_info a
    USING (SELECT t1.tmocif,
                  t1.tmprdc,
                  t1.tmtref,
                  t1.tmucy3,
                  t1.tmrvcb,
                  t1.tmoram,
                  t1.tmtenr,
                  t1.tmsrbr,
                  t1.tmpdtp,
                  t1.tmglgc,
                  t1.tmappn,
                  t1.tmfacs,
                  t1.tmblam,
                  t1.tmbosa,
                  t1.tmbosl,
                  t1.tmpmoa,
                  t1.tmstat,
                  t1.TMISSJ,
                  t1.tmexpj,
                  t1.TMOEXJ
           FROM   sync_tfmast t1,
                  bk_cif      t2
           WHERE  t1.tmocif = t2.cif_no) b
    ON (a.reference_no = b.tmtref)
    WHEN MATCHED THEN
      UPDATE
      SET    a.product_code  = b.tmprdc,
             a.balance_ccy   = TRIM(b.tmucy3),
             a.os_balance    = b.tmrvcb,
             a.ori_amt       = b.tmoram,
             a.issue_date    = DECODE(b.tmissj,
                                      0,
                                      null,
                                      TO_DATE(b.tmissj,
                                              'yyyyddd')),
             a.expiry_date   = decode(b.TMOEXJ,
                                      0,
                                      null,
                                      to_date(b.TMOEXJ,
                                              'yyyyddd')),
             a.tenor         = b.tmtenr,
             a.party_name    = NULL, --Chua xac dinh cua party_name
             a.branch        = b.tmsrbr,
             a.product_type  = TRIM(b.tmpdtp),
             a.gl_group_code = b.tmglgc,
             a.app_no        = TRIM(b.tmappn),
             a.facility      = b.tmfacs,
             a.bill_amt      = b.tmblam,
             --a.bill_os = b.tmbosa,
             a.lc_amt_local_equ = b.tmbosl,
             a.prev_month_os    = b.tmpmoa,
             a.acct_status      = b.tmstat
    WHEN NOT MATCHED THEN
      INSERT
        (a.reference_no,
         a.product_code,
         a.cif_no,
         a.balance_ccy,
         a.os_balance,
         a.ori_amt,
         a.issue_date,
         a.expiry_date,
         a.tenor,
         a.party_name,
         a.branch,
         a.product_type,
         a.gl_group_code,
         a.app_no,
         a.facility,
         a.bill_amt,
         --a.bill_os,
         a.lc_amt_local_equ,
         a.prev_month_os,
         a.acct_status)
      VALUES
        (b.tmtref,
         b.tmprdc,
         b.tmocif,
         TRIM(b.tmucy3),
         b.tmrvcb,
         b.tmoram,
         decode(b.tmissj,
                0,
                null,
                TO_DATE(b.tmissj,
                        'yyyyddd')),
         decode(b.TMOEXJ,
                0,
                null,
                to_date(b.TMOEXJ,
                        'yyyyddd')),
         b.tmtenr,
         NULL, --Chua xac dinh cua party_name
         b.tmsrbr,
         TRIM(b.tmpdtp),
         b.tmglgc,
         TRIM(b.tmappn),
         b.tmfacs,
         b.tmblam,
         --b.tmbosa,
         b.tmbosl,
         b.tmpmoa,
         b.tmstat);

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'ebank_tf_sync.proc_tf_info_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'ebank_tf_sync.proc_tf_info_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  PROCEDURE proc_tf_tran_sync IS
  BEGIN
    v_start_date := SYSDATE;

    INSERT INTO sync_tftran
      SELECT
      /*+ ALL_ROWS */
       xmprcd,
       xmprcj,
       xmtref,
       xmtxt1,
       xmtccy,
       xmtamt,
       xmbccy,
       xmbosa,
       xmpdtp,
       xmexpd,
       xmexpj,
       XMSRBR,
       XMOCIF
      FROM   svdatpv51.tftran@dblink_data;

    DELETE bb_tf_history
    WHERE  tran_date >= TRUNC(SYSDATE);

    INSERT INTO bb_tf_history
      (tran_date,
       reference_no,
       tran_desc,
       tran_ccy,
       tran_amt,
       --balance_ccy,
       os_balance,
       local_ccy,
       os_balance_lcy,
       product_type,
       expiry_date,
       source_branch,
       cif_no,
       CORE_SN)
      (SELECT TO_DATE(xmprcj,
                      'yyyyddd'),
              xmtref,
              TRIM(xmtxt1),
              TRIM(xmtccy),
              xmtamt,
              --TRIM(xmbccy),
              xmbosa,
              NULL,
              TRIM(XMBCCY),
              TRIM(xmpdtp),
              DECODE(xmexpj,
                     0,
                     NULL,
                     TO_DATE(xmexpj,
                             'yyyyddd')),
              XMSRBR,
              XMOCIF,
              xmtref
       FROM   sync_tftran
       WHERE  xmprcd <> 0
       AND    TO_DATE(xmprcj,
                      'yyyyddd') >= TRUNC(SYSDATE));

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    ebank_sync_util.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'ebank_tf_sync.proc_tf_tran_sync',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      ebank_sync_util.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'ebank_tf_sync.proc_tf_tran_sync',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  --Date: 11/05/2011
  --Comment: Sync tf product type
  --ManhNV
  PROCEDURE proc_tf_product_type IS
  BEGIN
    v_start_date := SYSDATE;

    DELETE FROM bb_tf_product_type;

    INSERT INTO bb_tf_product_type
      (product_type,
       group_code,
       product_name,
       description)
      (SELECT trim(a.PDPDTP),
              trim(a.PDPGPC),
              trim(a.PDSDES),
              trim(a.PDDESC)
       FROM   STAGING.SI_PAR_TFPAR2@STAGING_PRO_CORE a);

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_tf_product_type',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_tf_product_type',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

  --Date: 11/05/2011
  --Comment: Sync tf charge fee
  --ManhNV
  PROCEDURE proc_tf_charge IS
  BEGIN
    v_start_date := SYSDATE;

    INSERT INTO sync_tf_charg
      SELECT a.hcchtp,
             a.hctref,
             a.hctcur,
             a.hccamt,
             a.hcclcj
      FROM   STAGING.SI_DAT_TFHCHR@STAGING_PRO_CORE a;

    DELETE FROM bb_tf_charge;

    INSERT INTO bb_tf_charge
      (charge_type,
       reference_no,
       local_ccy,
       charge_amt,
       tran_date)
      (SELECT trim(a.hcchtp),
              trim(a.hctref),
              trim(a.hctcur),
              trim(a.hccamt),
              to_date(trim(a.hcclcj),
                      'yyyyddd')
       FROM   sync_tf_charg a);

    COMMIT;

    EXECUTE IMMEDIATE 'alter session close database link dblink_data';

    EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                  SYSDATE,
                                  'proc_tf_charge',
                                  NULL,
                                  'SUCC');
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      EBANK_SYNC_UTIL.proc_sync_log(v_start_date,
                                    SYSDATE,
                                    'proc_tf_charge',
                                    'SYSTEM BUSY'
                                    /* SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||SQLERRM,1,255) */,
                                    'FAIL');
      /*DBMS_OUTPUT.put_line(SUBSTR('Error ' || TO_CHAR(SQLCODE) || ': ' ||
      SQLERRM,
      1,
      255));*/
  END;

END;

/
