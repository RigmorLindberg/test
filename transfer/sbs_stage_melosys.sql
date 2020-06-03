create or replace package body dk_p.sbs_stage_melosys
as
/*
Laster data fra fagsak-forkammer til stageing-tabell for fagsak
*/
procedure p_stage_fagsak (i_log_level in varchar2 default 'INFO', i_initlast in number default 0)
is
	l_num_rows pls_integer := 0; 
 	l_param_lastet_dato timestamp;
  
	-- Konstanter til bruk for oppslag i parametertabellen
    gc_omraade     constant varchar2(30) := 'LOAD_SBS_PKG';
    gc_param_navn  constant varchar2(99) := 'SBS_STAGE_MELOSYS_FAGSAK';
  
begin

	lw.create_job(i_job_desc      => 'MELOSYS fagsak: leser fra forkammer og skriver til stagetabell.', 
                  i_job_log_level => i_log_level);


	lw.info('Start: p_stage_fagsak');

    if i_initlast = 1
      then l_param_lastet_dato := to_date('19000101','yyyymmdd');
                 lw.info('Kjører initlast');
      else l_param_lastet_dato := vedlikehold.load_by_partition_exchange.f_get_lastet_parameter(
                                    i_omraade        => gc_omraade,
                                    i_parameter_navn => gc_param_navn
                                    );
    end if;

  	lw.info('Start: p_stage_fagsak',l_param_lastet_dato);

    insert /*+ APPEND */ into dk_p.sbs_stage_fagsak_hist
    with person as (
        select 
		    mk_ident.ak_person1                       as ak_person1,
            mk_person_id_kilde.gyldig_fra_dato        as gyldig_fra_dato,
            mk_person_id_kilde.gyldig_til_dato        as gyldig_til_dato,
            mk_person_id_kilde.lk_person_id_kilde_num as lk_person_id_aktor
        from fk_person.mk_ident
        inner join fk_person.mk_person_id_kilde
          on mk_ident.person_id_off   = mk_person_id_kilde.person_id_off
         and mk_person_id_kilde.kilde = 'AKTOR'
    ), aktor as (
		select 
			saksnummer, aktoer_id, rolle, funksjonell_tid, 
			lead(funksjonell_tid,1,to_date('99991231','yyyymmdd'))
			over (partition by saksnummer order by funksjonell_tid, trans_tid, trans_id) as funksjonell_grense
		from fk_p.melosys_aktor_dvh
		where rolle = 'BRUKER'
	)
    select 
     -1
    ,fagsak.trans_id        		  
	,fagsak.fagsak_id        		  
	,aktor.aktoer_id
	,nvl(person.ak_person1,-1)        
	,fagsak.funksjonell_av        		  
	,fagsak.registrert_tid            
	,fagsak.trans_tid        		  
	,fagsak.funksjonell_tid           
	,case when fagsak.fagsak_type is null then 'UKJENT' else fagsak.fagsak_type end as fagsak_type
	,null -- FAGSAK_UNDER_KODE
	,fagsak.status           
	,fagsak.gsak_saksnummer        		
	,fagsak.lastet_dato
	,'sbs_stage_melosys_fagsak'            
	,fagsak.data_opphav
	,fagsak.kildesystem             
    from fk_p.melosys_fagsak_dvh fagsak
	left join aktor on aktor.saksnummer = fagsak.fagsak_id and aktor.funksjonell_tid <= fagsak.funksjonell_tid and fagsak.funksjonell_tid < aktor.funksjonell_grense
    left join person 
	   on cast(person.lk_person_id_aktor as varchar2(99)) = aktor.aktoer_id 
      and person.gyldig_fra_dato   <= fagsak.funksjonell_tid 
	  and fagsak.funksjonell_tid   <= gyldig_til_dato
    where fagsak.lastet_dato       >  l_param_lastet_dato;
	l_num_rows := sql%rowcount;
    commit;
	lw.info('End: p_stage_fagsak - number of rows', l_num_rows);
  
	lw.end_job(i_job_result => 'Antall rader lastet ' || l_num_rows);
EXCEPTION 
	WHEN OTHERS 
    THEN 
        lw.fatal('dbms_utility.format_call_stack', dbms_utility.format_call_stack);
        lw.end_job(i_job_result=>sqlcode || '- feilmelding -' ||sqlerrm, i_job_status=> 'Mislykket');
        RAISE;
  
end p_stage_fagsak;

/*
Laster data fra behandling-forkammer joined med fagsak-kjerne til stageing-tabell for behandling
*/
procedure p_stage_behandling (i_log_level in varchar2 default 'INFO', i_initlast in number default 0)
is
	l_num_rows pls_integer := 0; 
 	l_param_lastet_dato timestamp;
  
	-- Konstanter til bruk for oppslag i parametertabellen
    gc_omraade     constant varchar2(30) := 'LOAD_SBS_PKG';
    gc_param_navn  constant varchar2(99) := 'SBS_STAGE_MELOSYS_BEHANDLING';

begin

	lw.create_job(i_job_desc      => 'MELOSYS behandling: leser fra forkammer og skriver til stagetabell.', 
                  i_job_log_level => i_log_level);

	lw.info('Start: p_stage_behandling');

    if i_initlast = 1
      then l_param_lastet_dato := to_date('19000101','yyyymmdd');
           lw.info('Kjører initlast');
      else l_param_lastet_dato := vedlikehold.load_by_partition_exchange.f_get_lastet_parameter(
                                    i_omraade        => gc_omraade,
                                    i_parameter_navn => gc_param_navn
                                    );
    end if;

	lw.info('Start: p_stage_behandling',l_param_lastet_dato);

    insert /*+ APPEND */ into dk_p.sbs_stage_behandling_hist
    (
     pk_sbs_behandling_historikk
    ,lk_sbs_behandling_t
    ,lk_sbs_behandling
    ,lk_sbs_fagsak
    ,lk_sbs_behandling_relatert
    ,lk_sbs_behandling_vedtak
    ,fk_utenlandstilsnitt_fin
    ,venter_utland_flagg
    ,fk_sak_type
    ,fk_behandling_status
    ,fk_sak_resultat
    ,fk_ek_org_node
    ,utenlandstilsnitt_kode
    ,sak_type_kode
    ,behandling_status_kode
    ,resultat_kode
    ,behandling_kode
    ,lk_org_enhet
    ,avsluttet_flagg
    ,totrinn_flagg
    ,opprettet_trans_tid
    ,endret_av_kode
    ,funksjonell_tid
    ,mottatt_tid
    ,lastet_dato
    ,lastet_session
    ,data_opphav
    ,kildesystem
	,sekvens
    ,saksbehandler
    )
    with org as (
	
	 select distinct ek_vdh_org_node as ek_org_node, org_node_kode as lk_org_node
        from fk_p.vdh_org_node node        
        where org_node_type_kode = 'NORGENHET'
    ), beh_transformasjon as ( 
    select behandling.*
      	-- Stonad_kode transformasjon
        ,case when decode(behandling.status,'AVSLUTTET',1,0)=1 and
decode(resultat_type,'HENLEGGELSE',1,'FASTSATT_LOVVALGSLAND',1,'FORELOEPIG_FASTSATT_LOVVALGSLAND',1,'AVSLAG_MANGLENDE_OPPL',1,'REGISTRERT_UNNTAK',1,0)=1
		then 
		1 
		else 0 
		end as avsluttet_flagg
        ,case when behandling.enhet is null 
			then -4
			else 
			case when org.ek_org_node is null 
				then -1        	
				else org.ek_org_node 
			end 
		end as fk_ek_org_node               
    from fk_p.melosys_behandling_dvh behandling
    left join org on org.lk_org_node = behandling.enhet
	
    )
    select 
		-1
		,beh.trans_id					
		,beh.behandling_id				
		,beh.fagsak_id					
        ,null
		,null -- Trenger en annen måte å fastsette vedtak på. 
		-- setter fremmen nøkkeler i henhold til om koden er null eller ei og 
		-- om finnes relasjontabellen som det slåss opp mot. 
        ,nvl(ut.pk_utenlandstilsnitt_fin,-1)             as fk_utenlandstilsnitt_fin 
        ,case when bsf.behandling_status_fin_kode = 'VENTERPAAU' then 1 else 0 end as venter_utland_flagg           
        ,case when sak_type.pk_sak_type                   is null and beh.beh_type      is not null then -1 else nvl(sak_type.pk_sak_type                  ,-4) end as fk_sak_type
        ,case when behandling_status.pk_behandling_status is null and beh.status        is not null then -1 else nvl(behandling_status.pk_behandling_status,-4) end as fk_behandling_status           
        ,case when sak_resultat.pk_sak_resultat           is null and beh.resultat_type is not null then -1 else nvl(sak_resultat.pk_sak_resultat          ,-4) end as fk_sak_resultat                   
		,beh.fk_ek_org_node				
		,'UTLAND' --beh.utlandstilsnitt			 
		,beh.beh_type			
		,beh.status			
		,beh.resultat_type	
        ,beh_tema as behandling_kode
		,beh.enhet			
		,beh.avsluttet_flagg			
		-- konverterer felt fra j/n til 1/0 felet. 
		,1 --??? decode(lower(beh.totrinnsbehandling),'n',0,1) 
		,beh.trans_tid				            
		,beh.funksjonell_av				            
		,beh.funksjonell_tid 		            
		,cast(beh.registrert_tid as date) as mottatt_dato
        ,beh.lastet_dato
		,'sbs_stage_melosys_behandling'	        
		,beh.data_opphav			   
		,beh.kildesystem	
		,1 as sekvens
        ,funksjonell_av as saksbehandler
		-- alle joins er basert påunksjonell_tid, kode og kilde.
		-- alt dette bør være veldig standard.
    from beh_transformasjon beh
	left join DK_P.utenlandstilsnitt_fin ut 
		 on ut.utenlandstilsnitt_fin_kode = 'UTLAND'  
		and ut.kildesystem = beh.kildesystem 
		and ut.gyldig_fra_dato <= beh.funksjonell_tid 
		and beh.funksjonell_tid <= ut.gyldig_til_dato
    left join DK_P.sak_type sak_type 
		 on sak_type.sak_type_kode = beh.beh_type  
		and sak_type.kilde = beh.kildesystem 
		and sak_type.gyldig_fra_dato <= beh.funksjonell_tid 
		and beh.funksjonell_tid <= sak_type.gyldig_til_dato
    left join DK_P.sak_resultat sak_resultat 
		 on sak_resultat.sak_resultat_kode = beh.resultat_type  
		and sak_resultat.kilde = beh.kildesystem 
		and sak_resultat.gyldig_fra_dato <= beh.funksjonell_tid 
		and beh.funksjonell_tid <= sak_resultat.gyldig_til_dato
    left join DK_P.behandling_status behandling_status 
		 on behandling_status.behandling_status_kode = beh.status 
		and behandling_status.kilde = beh.kildesystem 
		and behandling_status.gyldig_fra_dato <= beh.funksjonell_tid 
		and beh.funksjonell_tid <= behandling_status.gyldig_til_dato
    left join DK_P.behandling_status_fin bsf 
         on behandling_status.fk_behandling_status_fin = bsf.pk_behandling_status_fin
    where beh.lastet_dato > l_param_lastet_dato ;
	l_num_rows := l_num_rows + sql%rowcount;
    commit;
	lw.info('End: p_stage_behandling - number of rows', l_num_rows);

	lw.end_job(i_job_result => 'Antall rader lastet ' || l_num_rows);
EXCEPTION 
	WHEN OTHERS 
    THEN 
        lw.fatal('dbms_utility.format_call_stack', dbms_utility.format_call_stack);
        lw.end_job(i_job_result=>sqlcode || '- feilmelding -' ||sqlerrm, i_job_status=> 'Mislykket');
        RAISE;
	end p_stage_behandling;
	procedure p_set_parameter 
	-- Endrer parameter etter last er ferdigstilt.
	is 
	begin 
		lw.info('Start: p_set_parameter','MELOSYS');
		vedlikehold.load_by_partition_exchange.p_set_parameters_lastet_dato(
				i_table          => 'sbs_behandling_historikk',
				i_owner          => 'DK_P',
				i_omraade        => 'LOAD_SBS_PKG',
				i_parameter_navn => 'SBS_STAGE_MELOSYS_BEHANDLING',
				i_kildesystem    => 'MELOSYS'
			);
		vedlikehold.load_by_partition_exchange.p_set_parameters_lastet_dato(
				i_table          => 'sbs_fagsak_historikk',
				i_owner          => 'DK_P',
				i_omraade        => 'LOAD_SBS_PKG',
				i_parameter_navn => 'SBS_STAGE_MELOSYS_FAGSAK',
				i_kildesystem    => 'MELOSYS'
			);
		lw.info('End: p_set_parameter','MELOSYS');
	end p_set_parameter;

end sbs_stage_melosys;