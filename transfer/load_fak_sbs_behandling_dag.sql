create or replace package body dt_p.load_fak_sbs_behandling_dag as
	-- Legges i tabell type parameter tabell.
	gc_part_tbl   constant varchar2(30) := 'FAK_SBS_STAGE_BEH_PART';
	gc_exch_tbl   constant varchar2(30) := 'FAK_SBS_BEHANDLING_EXCH';
	gc_target_tbl constant varchar2(30) := 'FAK_SBS_BEHANDLING_DAG';
	gc_owner      constant varchar2(30) := 'DT_P';
	gc_omraade    constant varchar2(30) := 'LOAD_SBS_PKG';
	gc_param_navn constant varchar2(99) := 'FAK_SBS_BEHANDLING_DAG';	

procedure p_load_stage_tbl(
     i_load_timer     in varchar2
	,i_relativ_maaned in dt_p.dim_tid.relativ_maaned%type
	)
is
	l_num_rows         pls_integer := 0; 
	l_antall_segmenter pls_integer; 
begin
	lw.info('Start: p_load_stage_tbl');
	lw.info('Relativ month of loading',i_relativ_maaned);

	if i_load_timer in ('HOURLY','DAILY')
    then
       l_antall_segmenter := 1;
       execute immediate 'alter session set parallel_degree_limit=16';
    else
        l_antall_segmenter := 150;
        execute immediate 'alter session set parallel_degree_limit=16';
	end if;

	for i in 1..l_antall_segmenter
    loop
	lw.trace('loop iterasjon', i ||' / '|| l_antall_segmenter);
	insert /*+ APPEND */ into dt_p.fak_sbs_stage_beh_part
	with tid_format as (
	select
		d.pk_dim_tid as fk_dim_tid_dag
		,cast(d.aar_uke||'002' as number(38))as fk_dim_tid_uke
		,cast(d.aar_maaned||'003' as number(38)) as fk_dim_tid_mnd
		,d.dato 
		,decode(d.dag_i_uke,7,1,0) as siste_i_uke_flagg
		,d.siste_i_maaned_flagg
	from dt_p.dim_tid d 
	where d.dim_nivaa         = 1 
	  and d.relativ_maaned   >= i_relativ_maaned
	  and d.relativ_dag      <= 0 
), behandling as (
    select historikk.*
    ,behandling.pk_sbs_behandling as fk_sbs_behandling 
    ,behandling.fk_ek_org_node_inngang as fk_dim_org_mottagende		
    ,behandling.inngang_tid		
    ,behandling.avsluttet_tid
    ,behandling.fk_ek_org_node_avsluttet as fk_dim_org_produksjon
    ,case when historikk.kildesystem = 'BISYS'
          then trunc(historikk.opprettet_trans_tid)
          else trunc(historikk.funksjonell_tid)
          end as fra_dato
    ,case when historikk.kildesystem = 'BISYS'
          then least(trunc(nvl(behandling.avsluttet_tid + 1, to_date('99991231','yyyymmdd'))),
               lead(trunc(historikk.opprettet_trans_tid),1,to_date('99991231','yyyymmdd') ) over 
	          (partition by historikk.kildesystem, historikk.lk_sbs_behandling
	           order by historikk.opprettet_trans_tid,historikk.sekvens))
          else least(trunc(nvl(behandling.avsluttet_tid + 1, to_date('99991231','yyyymmdd'))),
               lead(trunc(historikk.funksjonell_tid),1,to_date('99991231','yyyymmdd') ) over 
	          (partition by historikk.kildesystem, historikk.lk_sbs_behandling
	           order by historikk.funksjonell_tid,historikk.sekvens))
          end as til_dato
    ,behandling.slettet_flagg
    from dk_p.sbs_behandling behandling
    join dk_p.sbs_behandling_historikk historikk
      on historikk.lk_sbs_behandling = behandling.lk_sbs_behandling 
     and historikk.kildesystem = behandling.kildesystem
	where mod(regexp_substr(behandling.lk_sbs_fagsak, '\d+'), l_antall_segmenter) = i-1
      and historikk.funksjonell_tid <= nvl(behandling.avsluttet_tid, to_timestamp('99991231 01:00:00.000000', 'yyyymmdd hh24:mi:ss.ff'))
), fagsak as (
    select historikk.*
    ,fagsak.pk_sbs_fagsak as fk_sbs_fagsak
    ,trunc(historikk.funksjonell_tid) as fra_dato
    ,lead(trunc(historikk.funksjonell_tid),1,to_date('99991231','yyyymmdd') ) over 
		          (partition by historikk.lk_sbs_fagsak, historikk.kildesystem
		           order by historikk.funksjonell_tid) as til_dato
    from dk_p.sbs_fagsak fagsak
    join dk_p.sbs_fagsak_historikk historikk
      on historikk.lk_sbs_fagsak = fagsak.lk_sbs_fagsak 
     and historikk.kildesystem = fagsak.kildesystem
   where mod(regexp_substr(fagsak.lk_sbs_fagsak, '\d+'), l_antall_segmenter) = i-1
), org_varighet as (
    select dag.lk_sbs_behandling, dag.kildesystem, dag.fk_dim_org_ansvarlig_naa
        ,max(varighet_organisasjon) as max_varighet_org 
    from behandling
    join dt_p.fak_sbs_behandling_dag dag 
      on dag.lk_sbs_behandling = behandling.lk_sbs_behandling 
     and dag.kildesystem = behandling.kildesystem 
    where (
      trunc(behandling.inngang_tid) <= (select min(dato) from tid_format)
      or 
      (select min(dato) from tid_format) <= nvl(trunc(behandling.avsluttet_tid), to_date('99991231', 'yyyymmdd'))
      ) and dag.fk_dim_tid_dag < (select min(fk_dim_tid_dag) from tid_format)
    group by dag.lk_sbs_behandling, dag.kildesystem, dag.fk_dim_org_ansvarlig_naa
), behandling_dag as ( 
select 
   tid_format.fk_dim_tid_dag				
  ,tid_format.fk_dim_tid_uke      		
  ,tid_format.fk_dim_tid_mnd      		
  ,case when trunc(behandling.inngang_tid)   = tid_format.dato then 1                                   else 0    end as inngang_flagg
  ,case when trunc(behandling.mottatt_tid)   = tid_format.dato then 1                                   else 0    end as mottatt_flagg
  ,case when trunc(behandling.avsluttet_tid) = tid_format.dato then 1                                   else 0    end as produksjon_flagg
  ,case when trunc(behandling.inngang_tid) <= tid_format.dato and tid_format.dato < trunc(nvl(behandling.avsluttet_tid,to_date('99991231','yyyymmdd'))) then 1 else 0 end as restanse_flagg
  ,case when trunc(behandling.avsluttet_tid) = tid_format.dato then behandling.avsluttet_tid            else null end as produsert_tid
  ,case when trunc(behandling.avsluttet_tid) = tid_format.dato then behandling.fk_dim_org_produksjon    else -2   end as fk_dim_org_produksjon	
  ,fagsak.saksnummer_kode              as saksnummer
  ,behandling.lk_sbs_behandling			
  ,behandling.lk_sbs_behandling_vedtak    
  ,behandling.lk_sbs_fagsak				
  ,behandling.fk_sbs_behandling       
  ,nvl(fagsak.fk_sbs_fagsak,-1)        as fk_sbs_fagsak
  ,behandling.fk_sak_type              as fk_dim_f_sak_type			
  ,behandling.fk_sak_resultat          as fk_dim_f_resultat			
  ,behandling.fk_behandling_status     as fk_dim_f_behandling_status	
  ,behandling.fk_utenlandstilsnitt_fin 
  ,behandling.fk_dim_org_mottagende		
  ,behandling.fk_ek_org_node           as fk_dim_org_ansvarlig_naa 
  ,nvl(fagsak.fk_person1,-1)           as fk_person1               
  ,case when trunc(behandling.inngang_tid)   = tid_format.dato then 0 else behandling.venter_utland_flagg   end as venter_utland_flagg -- første dag skal ikke telle som venter
--  ,behandling.venter_utland_flagg
  ,tid_format.siste_i_uke_flagg		  
  ,tid_format.siste_i_maaned_flagg      		
  ,behandling.totrinn_flagg				              
  ,behandling.mottatt_tid               
  ,behandling.inngang_tid
  ,behandling.dato_for_uttak
  ,tid_format.dato
  ,behandling.utenlandstilsnitt_kode
  ,behandling.beslutter	
  ,behandling.saksbehandler 
  ,behandling.kildesystem
  ,behandling.lastet_dato
  ,case when fagsak.fagsak_kode in ('ES','FP') and behandling.kildesystem = 'FPSAK'  then nvl(fagsak.fagsak_kode,'') || nvl(behandling.behandling_kode,'-')
        when behandling.kildesystem = 'MELOSYS'  then nvl(fagsak.fagsak_kode,'')||'_'|| nvl(behandling.behandling_kode,'-')
      else nvl(fagsak.fagsak_kode,'') || behandling.behandling_kode end as stonad_kode
from behandling 
join tid_format 
  on behandling.fra_dato <= tid_format.dato 
 and tid_format.dato < behandling.til_dato 
left join fagsak 
  on fagsak.lk_sbs_fagsak = behandling.lk_sbs_fagsak 
 and fagsak.kildesystem = behandling.kildesystem
 and fagsak.fra_dato <= tid_format.dato 
 and tid_format.dato < fagsak.til_dato 
where behandling.slettet_flagg = 0 or behandling.lastet_dato >= tid_format.dato
), person as (
select
   fk_person1
  ,fk_dim_kjonn
  ,fk_dim_geografi_bosted
  ,gyldig_fra_dato
  ,case when gyldig_til_dato >= gyldig_fra_dato_neste then gyldig_fra_dato_neste -1 else gyldig_til_dato end gyldig_til_dato
  ,utfaset
 from ( select
           fk_person1
          ,fk_dim_kjonn
          ,fk_dim_geografi_bosted
          ,gyldig_fra_dato
          ,gyldig_til_dato
          ,gyldig_flagg
          ,lastet_dato
          ,oppdatert_dato
          ,utfaset
          ,nvl(lead(gyldig_fra_dato) over (partition by fk_person1
                                           order by gyldig_fra_dato
                                                  , gyldig_til_dato), to_date('99991231','yyyymmdd')) gyldig_fra_dato_neste
       from dt_p.dim_person
       where nvl(utfaset,0) = 0)
)
select  
   dag.fk_dim_tid_dag				
  ,dag.fk_dim_tid_uke      		
  ,dag.fk_dim_tid_mnd      		
  ,dag.saksnummer					
  ,dag.lk_sbs_behandling			
  ,dag.lk_sbs_behandling_vedtak    
  ,dag.lk_sbs_fagsak				
  ,dag.fk_sbs_behandling    		
  ,dag.fk_sbs_fagsak				
  ,dag.fk_dim_f_sak_type			
  ,dag.fk_dim_f_resultat			
  ,dag.fk_dim_f_behandling_status	
  ,case when dag.stonad_kode is not null then nvl(stonad.pk_stonad,-4) else -1 end as fk_dim_f_stonad_omraade		
  ,nvl(utenland.pk_dim_utenlandstilsnitt,-4) as fk_dim_utenlandstilsnitt
  ,nvl(person.fk_dim_kjonn,-1)
  ,nvl(person.fk_dim_geografi_bosted,-1)
  ,dag.fk_dim_org_mottagende		
  ,dag.fk_dim_org_produksjon		
  ,dag.fk_dim_org_ansvarlig_naa 
  ,dag.fk_person1               
  ,venter_utland_flagg
  ,case when dag.produksjon_flagg=1 then 1 else dag.siste_i_uke_flagg end as siste_i_uke_flagg		  
  ,case when dag.produksjon_flagg=1 then 1 else dag.siste_i_maaned_flagg end as siste_i_maaned_flagg		  
  ,dag.mottatt_flagg				
  ,dag.inngang_flagg				
  ,dag.produksjon_flagg			
  ,dag.restanse_flagg				
  ,dag.totrinn_flagg
  ,case when dag.fk_dim_org_ansvarlig_naa != lead(
        dag.fk_dim_org_ansvarlig_naa -- ,1,case when dag.produksjon_flagg = 1 then dag.fk_dim_org_produksjon else dag.fk_dim_org_ansvarlig_naa end
        ) over (
            partition by dag.lk_sbs_behandling, dag.kildesystem order by dag.fk_dim_tid_dag
        ) then 1 else 0 end as oversendt_flagg
  ,case when dag.fk_dim_org_ansvarlig_naa != lag(
        dag.fk_dim_org_ansvarlig_naa -- ,1,case when dag.inngang_flagg = 1 then dag.fk_dim_org_mottagende else dag.fk_dim_org_ansvarlig_naa end
        ) over (
            partition by dag.lk_sbs_behandling, dag.kildesystem order by dag.fk_dim_tid_dag
        ) then 1 else 0 end as tilsendt_flagg
  ,dag.stonad_kode               
  ,dag.mottatt_tid               
  ,dag.inngang_tid
  ,dag.dato_for_uttak
  ,dag.produsert_tid	          
  ,dag.dato - trunc(nvl(dag.mottatt_tid, dag.inngang_tid)) + 1 as varighet_dager
  ,nvl(varighet.max_varighet_org,0)  + row_number() over (partition by dag.lk_sbs_behandling, dag.kildesystem, dag.fk_dim_org_ansvarlig_naa order by dag.fk_dim_tid_dag) as varighet_organisasjon
  ,dag.beslutter	
  ,dag.saksbehandler 
  ,dag.lastet_dato					
  ,'load_fak_sbs_behandling' as lastet_session              
  ,dag.kildesystem					
from behandling_dag dag
left join org_varighet varighet
 on varighet.lk_sbs_behandling = dag.lk_sbs_behandling
and varighet.fk_dim_org_ansvarlig_naa = dag.fk_dim_org_ansvarlig_naa
and varighet.kildesystem = dag.kildesystem
left join dk_p.stonad stonad 
  on stonad.stonad_kode=dag.stonad_kode 
 and dag.dato between stonad.gyldig_fra_dato 
 and stonad.gyldig_til_dato 
 and dag.kildesystem = stonad.kilde
left join dt_p.dim_utenlandstilsnitt utenland
  on utenland.utenlandstilsnitt_fin_kode = dag.utenlandstilsnitt_kode 
 and dag.kildesystem = utenland.kildesystem 
 and utenland.gyldig_fra_dato <= dag.dato 
 and dag.dato <= utenland.gyldig_til_dato
-- Hack pga overlapp i dim_person
left join person -- dt_p.dim_person person
  on person.fk_person1 = dag.fk_person1 
 and person.gyldig_fra_dato <= dag.dato 
 and dag.dato <= person.gyldig_til_dato
 and nvl(utfaset,0) = 0
;

	l_num_rows := l_num_rows + sql%rowcount;
	commit;
	end loop;
	lw.info('Ferdig loop', l_num_rows);
	vedlikehold.load_by_partition_exchange.p_tbl_maintenance(
		i_tbl_name =>gc_part_tbl
		,i_owner   =>gc_owner);

	lw.info('Slutt: p_load_stage_tbl number of rows', l_num_rows);
end p_load_stage_tbl;

function f_get_parameters_relativ_mnd(
	 i_lastet_tid in timestamp
	,i_initlast   in integer default 0
	)
return dt_p.dim_tid.relativ_maaned%type
is 
	l_relativ_maaned dt_p.dim_tid.relativ_maaned%type;
begin 
	lw.info('Start: f_get_parameters_relativ_mnd with lastet_tid',i_lastet_tid);
	
	-- Kommenter month et eller annet...
	if i_initlast=1 then 
		select m.relativ_maaned into l_relativ_maaned
		from dt_p.dim_tid m
		join dt_p.dim_tid d 
		  on d.aar_maaned = m.aar_maaned
		where 1=1 
		  and d.dato = (to_date('20000101','yyyymmdd'))
		  and d.dim_nivaa = 1
		  and m.dim_nivaa = 3 
		;
	else 
		with dk_utsnitt as (
			select kildesystem, funksjonell_tid, lastet_dato from dk_p.sbs_behandling_historikk
			union all 
			select kildesystem, funksjonell_tid, lastet_dato from dk_p.sbs_fagsak_historikk
		), first_funk_tid as (
			select min(funksjonell_tid) as funksjonell_tid
			from dk_utsnitt 
			where lastet_dato >= i_lastet_tid
			group by kildesystem
		)
		select m.relativ_maaned into l_relativ_maaned
		from dt_p.dim_tid m
		join dt_p.dim_tid d on d.aar_maaned = m.aar_maaned
			where d.dato = (select trunc(min(funksjonell_tid)) from first_funk_tid)
			and d.dim_nivaa = 1
			and m.dim_nivaa = 3 
		;
		
	end if;
	lw.info('slutt: f_get_parameters_relativ_mnd',l_relativ_maaned);
	return l_relativ_maaned;
exception 
	when others then 
	select m.relativ_maaned into l_relativ_maaned
    from dt_p.dim_tid m
    join dt_p.dim_tid d on d.aar_maaned = m.aar_maaned
        where d.dato = to_date('20000101','yyyymmdd')
          and d.dim_nivaa = 1
          and m.dim_nivaa = 3 
    ;
	lw.info('Slutt: f_get_parameters_relativ_mnd - initial load',l_relativ_maaned);
	return l_relativ_maaned;
end f_get_parameters_relativ_mnd;

procedure p_drop_indexes(
     i_load_timer in varchar2
	)
is
    type varchar2_arr is varray(3) of varchar2(100);
    drop_table_arr varchar2_arr := varchar2_arr('drop index pk_'||gc_part_tbl
                                               ,'drop index pk_'||gc_exch_tbl
                                               ,'drop index pk_'||gc_target_tbl);
begin
    if i_load_timer = 'WEEKLY' then
       for i in 1..drop_table_arr.count
       loop
       begin
           execute immediate drop_table_arr(i);
       exception
       when others then
           if sqlcode not in (-1418) then
              raise;
           end if;
       end;
       end loop;
    end if;
end p_drop_indexes;

procedure p_create_indexes(
     i_load_timer in varchar2
	)
is
begin
    if i_load_timer = 'WEEKLY' then
       execute immediate 'create unique index pk_'||gc_part_tbl||' on '||gc_part_tbl||' (fk_dim_tid_dag, lk_sbs_behandling, kildesystem) local';
       execute immediate 'create unique index pk_'||gc_exch_tbl||' on '||gc_exch_tbl||' (fk_dim_tid_dag, lk_sbs_behandling, kildesystem)';
       execute immediate 'create unique index pk_'||gc_target_tbl||' on '||gc_target_tbl||' (fk_dim_tid_dag, lk_sbs_behandling, kildesystem) local';
    end if;
end p_create_indexes;

procedure p_main (
     i_load_timer in varchar2 default 'HOURLY'
	,i_log_level  in varchar2 default 'INFO'
	,i_initlast   in integer default 0
	)
is
    l_relativ_maaned   dt_p.dim_tid.relativ_maaned%type;
	l_param_lastet_tid timestamp;
	l_load_num_rows    pls_integer;
begin 
	lw.create_job(i_job_desc => 'Torglast (felles saksbeh.stat), med partition exchange til tabell '||gc_owner||'.'||gc_target_tbl, i_job_log_level => i_log_level);

	lw.trace('gc_part_tbl',gc_part_tbl);
	lw.trace('gc_exch_tbl',gc_exch_tbl);
	lw.trace('gc_target_tbl',gc_target_tbl);
	lw.trace('gc_owner',gc_owner);
	lw.trace('gc_omraade', gc_omraade);
	lw.trace('gc_param_navn',gc_param_navn);

	vedlikehold.load_by_partition_exchange.p_tbl_resett(
		 i_tbl_name => gc_part_tbl
		,i_owner    => gc_owner
		);

    l_param_lastet_tid := vedlikehold.load_by_partition_exchange.f_get_lastet_parameter(
		 i_omraade        => gc_omraade
		,i_parameter_navn => gc_param_navn
		);

	l_relativ_maaned := f_get_parameters_relativ_mnd(
		 i_lastet_tid=> l_param_lastet_tid
		,i_initlast  => i_initlast
		);

    p_drop_indexes(
        i_load_timer
       );

    p_load_stage_tbl(
        i_load_timer     => i_load_timer
	   ,i_relativ_maaned => l_relativ_maaned
	   );

	vedlikehold.load_by_partition_exchange.p_partition_exchange(
	     i_tbl_src        => gc_part_tbl
        ,i_tbl_tgt        => gc_target_tbl
        ,i_tbl_exch       => gc_exch_tbl
        ,i_owner          => gc_owner
	    ,i_log_level      => i_log_level
	    ,o_total_num_rows => l_load_num_rows
		);

    p_create_indexes(
        i_load_timer
       );

    vedlikehold.load_by_partition_exchange.p_set_parameters_lastet_dato(
		 i_table => gc_target_tbl
		,i_owner => gc_owner
		,i_omraade => gc_omraade
		,i_parameter_navn => gc_param_navn
		);

	lw.end_job(i_job_result => 'Antall rader lastet ' || l_load_num_rows);
EXCEPTION 
	WHEN OTHERS 
    THEN 
		lw.call_stack;
        lw.fatal('dbms_utility.format_call_stack', dbms_utility.format_call_stack);
        lw.end_job(i_job_result=>sqlcode || '- feilmelding -' ||sqlerrm, i_job_status=> 'Mislykket');
        RAISE;
        -- Lage ora kode med teks??
end p_main;

end load_fak_sbs_behandling_dag;
/

