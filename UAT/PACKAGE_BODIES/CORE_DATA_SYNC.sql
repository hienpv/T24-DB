--------------------------------------------------------
--  DDL for Package Body CORE_DATA_SYNC
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE BODY "CORE_DATA_SYNC" AS

  PROCEDURE ddrbal_sync AS
  
  v_TRDATE NUMBER(20,0) := 0;
  v_TRETIM NUMBER(10,0) := 0;
  v_TRACCT NUMBER(20,0) := 0;
  v_TRPODR NUMBER(10,0) := 0;
  v_last_TRDATE NUMBER(20,0) := 0;
  v_last_TRETIM NUMBER(10,0) := 0;
  v_last_TRACCT NUMBER(20,0) := 0;
  v_last_TRPODR NUMBER(10,0) := 0;
  v_curr_date varchar2(20);
  v_counter number(5,0) := 0;
  v_query varchar2(4000);
  v_max_id number(20,0);
  v_new_max_id number(20,0);
       
  BEGIN
  
  SELECT TO_CHAR(SYSDATE, 'yyyyDDD') into v_curr_date from DUAL;
  select trdate, tretim, tracct, trpodr into  v_TRDATE, v_TRETIM, v_TRACCT, v_TRPODR from ddrbal_check_point;
  select max(id) into v_max_id from SI_DAT_DDRBAL ;
  
  dbms_output.put_line('v_curr_date ' || v_curr_date);
  dbms_output.put_line('v_TRDATE ' || v_TRDATE || ' , v_TRETIM ' || v_TRETIM || ' , v_TRACCT ' || v_TRACCT || ' , v_TRPODR ' || v_TRPODR);
  dbms_output.put_line('v_max_id ' || v_max_id);
  
   v_query :=  '  INSERT INTO si_dat_ddrbal (id ,
    trstat,    stack,    batch,    seq,    trchkp,    serial,    route,    tracct,
    tratyp,    trbr,    trsobr,    dorc,    bori,    afdola,    pstseq,    afrtdr,
    afrtcr,    trancd,    amt,    runbal,    trctyp,    trcdec,    camt,    trcurr,
    source,    treffd,    treff6,    tretim,    trdate,    trdat6,    trtime,    truser,
    trsupv,    trjobn,    trjbno,    eftacc,    eftact,    efttyp,    fpay,    stops,
    scitem,    timnsf,    mmddr,    cntenc,    listpo, TRRATE,    trexcc,
    auxtrc,    trbopc,    trpodr,    chgflg,    nobook,    dtitcd,    ddgrup,
    dmcost,    dmprdc,    inacut,    trrsv1,    trrsv2,    trrsva,    trefth,
    trusr2,    trjsq2,    trtktn,    trcorf,    change_time )  
	select  SI_DAT_DDRBAL_SEQ.NEXTVAL, trim(trstat),    trim(stack),    batch,    seq,    trim(trchkp),    serial,    route,    tracct,
    trim(tratyp),    trbr,    trsobr,    trim(dorc),    trim(bori),    trim(afdola),    trim(pstseq),    trim(afrtdr),
    trim(afrtcr),    trancd,    amt,    runbal,    trim(trctyp),    trim(trcdec),    camt,    trim(trcurr),
    trim(source),    treffd,    treff6,    tretim,    trdate,    trdat6,    trtime,    trim(truser),
    trim(trsupv),    trim(trjobn),    trjbno,   eftacc,    trim(eftact),    trim(efttyp),    trim(fpay),    trim(stops),
    trim(scitem),    trim(timnsf),    trim(mmddr),    trim(cntenc),    trim(listpo), TRRATE,    trim(trexcc),
    trim(auxtrc),    trbopc,    trpodr,    trim(chgflg),    trim(nobook),    dtitcd,    ddgrup,
    dmcost,    dmprdc,    trim(inacut),    trim(trrsv1),    trim(trrsv2),    trim(trrsva),    trim(trefth),
    trim(trusr2),    trjsq2,    trim(trtktn),    trim(trcorf),    sysdate 
 from (
            select  a.* 
            from STHISTRN.DDRBAL@dblink_core249 a 
            where a.TRDATE = ' || v_curr_date || ' and a.TRTIME >= 0  and (( ' || v_TRDATE || ' = '  || v_curr_date || ' and 
            ( (a.TRTIME = ' || v_TRETIM || ' and 
                 ((a.TRACCT = ' || v_TRACCT || ' and (a.TRPODR > ' || v_TRPODR || ' ) ) or ( a.TRACCT > ' || v_TRACCT || ') )  )
                 or (a.TRTIME > ' || v_TRETIM || ')  )  )
            or ( ' || v_TRDATE || ' <>  ' || v_curr_date || ' )  )
			and exists (select 1 from bk_account_info b where a.tracct = ltrim(b.ACCT_NO, ''0'') )
            order by a.TRDATE, a.TRTIME, a.TRACCT, a.TRPODR asc) t  where rownum < 3000 ';
            
      execute immediate v_query;
      commit;
      
      select max(id) into v_new_max_id from SI_DAT_DDRBAL ;
      if (v_new_max_id = v_max_id) then
            dbms_output.put_line('no new record ' || v_max_id);
            return;
      end if;
        
      -- update check point
      select trdate, tretim, tracct, trpodr into  v_last_TRDATE, v_last_TRETIM, v_last_TRACCT, v_last_TRPODR 
      from SI_DAT_DDRBAL where id = v_new_max_id;
      
       update ddrbal_check_point a set a.trdate = v_last_TRDATE, a.TRETIM = v_last_TRETIM, 
             a.TRACCT = v_last_TRACCT, a.TRPODR = v_last_TRPODR, a.change_time = sysdate;      
       commit;
    
  END ddrbal_sync;

END CORE_DATA_SYNC;

/
