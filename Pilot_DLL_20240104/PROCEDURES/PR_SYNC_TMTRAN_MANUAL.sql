--------------------------------------------------------
--  DDL for Procedure PR_SYNC_TMTRAN_MANUAL
--------------------------------------------------------
set define off;

  CREATE OR REPLACE EDITIONABLE PROCEDURE "IBS"."PR_SYNC_TMTRAN_MANUAL" (p_etl_date NUMBER, p_from_time NUMBER, p_to_time NUMBER)
is
    cursor  c_txn
        is
            SELECT * FROM
            (
                SELECT (case when C.rollout_acct_no is null then 'I' ELSE 'U' END) ioru_ind, a.*
                FROM
                (
                    select TRIM(tmtellid) tmtellid, tmtxseq, tmentdt7, tmacctno, TRIM(tmdorc) tmdorc, tmtxamt,
                    TRIM(tmhosttxcd) tmhosttxcd, tmsseq,
                    DECODE(TRIM(tmtxstat),
                                'CE',
                                'FAIL',
                                'SUCC') tmtxstat
                    FROM  twtb_si_dat_tmtran
                    MINUS
                    SELECT teller_id, tm_seq tmtxseq, TO_NUMBER(TO_CHAR(c.post_time, 'RRRRDDD')) tmentdt7,
                    TO_NUMBER(rollout_acct_no) tmacctno,
                    dc_sign tmdorc,
                    amount tmtxamt,
                    device_no tmhosttxcd,
                    TO_NUMBER(tran_device) tmsseq,
                    status tmtxstat
                    FROM IBS.bk_account_history C
                    WHERE c.tran_time >= TO_DATE( p_etl_date || LPAD(p_from_time, 6, '0'), 'RRRRDDDHH24MISS')
                    AND  c.tran_time <= TO_DATE( p_etl_date || LPAD(p_to_time, 6, '0'), 'RRRRDDDHH24MISS')
                )MI
                INNER JOIN twtb_si_dat_tmtran a --de ra full thong tin
                    ON TRIM(a.tmtellid) = mi.tmtellid
                    AND a.tmtxseq = mi.tmtxseq
                    AND a.tmentdt7 = mi.tmentdt7
                    AND a.tmacctno = mi.tmacctno
                    AND TRIM(a.tmdorc) = mi.tmdorc
                    AND a.tmtxamt = mi.tmtxamt
                    AND TRIM(a.tmhosttxcd) = mi.tmhosttxcd
                    And a.tmsseq = mi.tmsseq
                LEFT JOIN IBS.bk_account_history C --de check giao dich updat or insert
                ON c.tran_time >= TO_DATE( p_etl_date || LPAD(p_from_time, 6, '0'), 'RRRRDDDHH24MISS')
                    AND  c.tran_time <= TO_DATE( p_etl_date || LPAD(p_to_time, 6, '0'), 'RRRRDDDHH24MISS')
                    AND c.teller_id = mi.tmtellid
                    AND c.tm_seq  = mi. tmtxseq
                    AND TO_NUMBER(C.rollout_acct_no) = MI.tmacctno
                    AND C.dc_sign = MI.tmdorc
                    AND C.amount = MI.tmtxamt
                    AND C.device_no = MI.tmhosttxcd
                    AND TO_NUMBER(C.tran_device) = MI.tmsseq
                    AND C.status  = MI.tmtxstat
            ) X
            WHERE (case
                        when X.IORU_IND = 'I' then 'P'
                  ELSE
                    CASE WHEN TRIM(X.tmtxstat) = 'CE' THEN 'P' ELSE 'N' END
                  END ) = 'P'
        /*
        select * From si_dat_tmtran@rawstage_pro
        where tmentdt7 = p_etl_date--2016145
        and tmtiment >= p_from_time--113925
        and tmtiment < p_to_time--154906
        order by tmtiment
        */
        ;

    --type ty_row is record of cur%rowtype;
    type ty_data is table of c_txn%rowtype index by PLS_INTEGER;
    l_txn_list ty_data;

begin

    EXECUTE IMMEDIATE 'TRUNCATE TABLE twtb_si_dat_tmtran';

    INSERT INTO twtb_si_dat_tmtran(tmstack, tmtxstat, tmoffset, tmmemo, tmlatloc, tmibttrn, tmeqvtrn, tmsumtrn, tmtlxmne, tmtellid, tmtxseq, tmusr2, tmjsq2, tmtxcd, tmsupid, tmentdat, tmentdt7, tmeffdat, tmeffdt7, tmsrvbrn, tmactbrn, tmapptype, tmhosttxcd, tmtiment, tmtxsrc, tmorgamt, tmorgcur, tmtxamt, tmglcur, tmbrneqv, tmbrncur, tmbnkeqv, tmbnkcur, tmacctno, tmglcost, tmglprod, tmchkp, tmserial, tmroute, tmdorc, tmhafft, tmeftnum, tmefttyp, tmbopcod, tmsrcfnd, tmchgtyp, tmlnrev, tmafftrel, tmsrunit, tmsrval, tmsrcntr, tmdesc, tmrefnum, tmexcode, tmtpneft, tm3rdptyn, tmnbrl, tmeftcod, tmcont, tmvaldat, tmvaldt7, tmnchk, tmglexch, tmtxflg1, tmtxnrate, tmsseq, tmbkdat6, tmbkdat7, tmefth, tmresv01, tmresv02, tmresv03, tmresv04, tmresv05, tmtktn, tmresv06, tmresv07)
    SELECT tmstack, tmtxstat, tmoffset, tmmemo, tmlatloc, tmibttrn, tmeqvtrn, tmsumtrn, tmtlxmne, tmtellid, tmtxseq, tmusr2, tmjsq2, tmtxcd, tmsupid, tmentdat, tmentdt7, tmeffdat, tmeffdt7, tmsrvbrn, tmactbrn, tmapptype, tmhosttxcd, tmtiment, tmtxsrc, tmorgamt, tmorgcur, tmtxamt, tmglcur, tmbrneqv, tmbrncur, tmbnkeqv, tmbnkcur, tmacctno, tmglcost, tmglprod, tmchkp, tmserial, tmroute, tmdorc, tmhafft, tmeftnum, tmefttyp, tmbopcod, tmsrcfnd, tmchgtyp, tmlnrev, tmafftrel, tmsrunit, tmsrval, tmsrcntr, tmdesc, tmrefnum, tmexcode, tmtpneft, tm3rdptyn, tmnbrl, tmeftcod, tmcont, tmvaldat, tmvaldt7, tmnchk, tmglexch, tmtxflg1, tmtxnrate, tmsseq, tmbkdat6, tmbkdat7, tmefth, tmresv01, tmresv02, tmresv03, tmresv04, tmresv05, tmtktn, tmresv06, tmresv07
    FROM rawstage.si_dat_tmtran@rawstage_pro_core
        WHERE tmapptype <> 'G'
        AND tmentdt7 = p_etl_date--2016145
        and tmtiment >= p_from_time--113925
        and tmtiment <= p_to_time--154906
        ;

    COMMIT;

    open c_txn;
    LOOP
        FETCH c_txn
        BULK COLLECT INTO l_txn_list
        LIMIT  10000;

        FOR i IN 1..l_txn_list.count
        LOOP
            IF l_txn_list(i).IORU_IND = 'I' THEN
                ibs.cspkg_transaction_sync.pr_tmtran_sync ('I',
                                    l_txn_list(i).tmtxcd,
                                    l_txn_list(i).tmresv07,
                                    l_txn_list(i).tmdorc,
                                    l_txn_list(i).tmtxamt,
                                    l_txn_list(i).tmglcur,
                                    l_txn_list(i).tmorgamt,
                                    l_txn_list(i).tmefth,
                                    l_txn_list(i).tmacctno,
                                    l_txn_list(i).tmtellid,
                                    l_txn_list(i).tmtxseq,
                                    l_txn_list(i).tmtxstat,
                                    l_txn_list(i).tmhosttxcd,
                                    l_txn_list(i).tmapptype,
                                    l_txn_list(i).tmeqvtrn,
                                    l_txn_list(i).tmibttrn,
                                    l_txn_list(i).tmsumtrn,
                                    l_txn_list(i).tmentdt7,
                                    l_txn_list(i).tmeffdt7,
                                    l_txn_list(i).tmsseq,
                                    l_txn_list(i).tmtiment);
            ELSE

                ibs.cspkg_transaction_sync.pr_tmtran_fail_sync ('U',
                                   l_txn_list(i).tmtxcd,
                                    l_txn_list(i).tmresv07,
                                    l_txn_list(i).tmdorc,
                                    l_txn_list(i).tmtxamt,
                                    l_txn_list(i).tmglcur,
                                    l_txn_list(i).tmorgamt,
                                    l_txn_list(i).tmefth,
                                    l_txn_list(i).tmacctno,
                                    l_txn_list(i).tmtellid,
                                    l_txn_list(i).tmtxseq,
                                    l_txn_list(i).tmtxstat,
                                    l_txn_list(i).tmhosttxcd,
                                    l_txn_list(i).tmapptype,
                                    l_txn_list(i).tmeqvtrn,
                                    l_txn_list(i).tmibttrn,
                                    l_txn_list(i).tmsumtrn,
                                    l_txn_list(i).tmentdt7,
                                    l_txn_list(i).tmeffdt7,
                                    l_txn_list(i).tmsseq,
                                    l_txn_list(i).tmtiment);

            END IF;

        END LOOP;

        EXIT WHEN c_txn%NOTFOUND;

    END LOOP;
    CLOSE c_txn ;

exception
    when others THEN
        CLOSE c_txn;
        raise;


end;

/
