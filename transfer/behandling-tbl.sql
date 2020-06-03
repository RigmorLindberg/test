
whenever sqlerror exit failure;
-- droper tabeller og seksvenser om de eksisterer fra før.
set echo off
declare
   type varchar2_arr is varray(8) of varchar2(100);
   drop_table_arr varchar2_arr := varchar2_arr(
											   'drop table dk_p.sbs_behandling_historikk cascade constraint purge'
											  ,'drop table dk_p.sbs_behandling_historikk_exch purge'
                                              ,'drop table dk_p.sbs_behandling_slettet purge'
											  ,'drop table dk_p.sbs_stage_behandling_hist_part purge'
                                              ,'drop table dk_p.sbs_stage_behandling_hist purge'
                                              ,'drop table dk_p.sbs_stage_behandling_hist_dl purge'
                                              ,'drop table dk_p.sbs_stage_behandling_hist_fl purge'
                                              ,'drop sequence dk_p.seq_sbs_behandling_historikk'
                                              );
begin
   for i in 1..drop_table_arr.count loop
   begin
      execute immediate drop_table_arr(i);
   exception
   when others then
      if sqlcode not in (-942,-2289) then
         raise;
      end if;
   end;
   end loop;
end;
/
set echo on

-- lager primær nøkkel.
create sequence dk_p.seq_sbs_behandling_historikk cache 1000;

-- lager kjerne tabell.
create table dk_p.sbs_behandling_historikk (
	-- se fagsak for mer detaljer
	pk_sbs_behandling_historikk    number(38)    not null,
	lk_sbs_behandling_t  		   varchar2(38)  not null,
	lk_sbs_behandling    		   varchar2(38)  not null,
	lk_sbs_fagsak                  varchar2(38)  not null,
    lk_sbs_behandling_relatert     varchar2(38) ,
	lk_sbs_behandling_vedtak       varchar2(38) ,
	-- pseudonymous foreign key
	fk_utenlandstilsnitt_fin 	   number(38)	not null, -- her bør det kanskje avgjøres noe?
	fk_sak_type	                   number(38)   not null,
	--fk_stonad                      number(38)   not null,
	fk_behandling_status           number(38)   not null,
	fk_sak_resultat                number(38)   not null,
	fk_ek_org_node                 number(38)   not null,
	sekvens						   number(38)  ,
	-- codes describing distributional factors according to the source
	behandling_kode                varchar2(100), -- dette er da en ferdig "beregnede stønadskoden" for å finne opphav til koden må forkammeret gi dette. 
	utenlandstilsnitt_kode		   varchar2(40), -- vanlig utenlandtilsnitt 
	sak_type_kode                  varchar2(100), -- behandlingstype 
	behandling_status_kode         varchar2(40), -- behandlingsstatus - her brukte vi behandlingssteg før men dette bør utgå. 
    venter_utland_flagg            number(38) ,
	resultat_kode                  varchar2(40), -- behandlingsresultat 
	lk_org_enhet                   varchar2(40), -- organisasjonsenhetennummeret
	-- functional data extracted from the source
	avsluttet_flagg				   number(1) not null, -- flagget identifiserer om status koden indikerer at behandlingen er ferdig. 
	totrinn_flagg                  number(1) not null,  
	-- time related data for creation
	opprettet_trans_tid            timestamp(6)  not null,
	endret_av_kode                 varchar2(40) ,
	beslutter	  varchar2(40) ,
	saksbehandler varchar2(40) ,
	funksjonell_tid                timestamp(6)  not null,
	mottatt_tid				       timestamp(6) ,
	--registrert_tid   			   timestamp(6) ,
    dato_for_uttak                 timestamp(6) ,
	-- meta data as implemented by the datawarehouse standard
	lastet_dato                    timestamp(6) not null,  
	lastet_session                 varchar(40)  not null,
	data_opphav					   varchar(40)  not null, 
	kildesystem                    varchar2(10) not null
) column store compress for query high
partition by range(funksjonell_tid) interval(numtoyminterval(1,'month'))
( partition p0 values less than ( to_date('20060101','yyyymmdd') ) )
;	

grant select on dk_p.sbs_behandling_historikk to dvh_dk_p_ro_role;
grant insert on dk_p.sbs_behandling_historikk to dvh_dk_p_rw_role;
grant select on dk_p.sbs_behandling_historikk to dt_P with grant option;--brukes av dt_p.KLAGE_DETALJER

create table  dk_p.sbs_behandling_slettet (
	lk_sbs_behandling varchar2(100 byte)	not null,
	uttrekk_dato	 timestamp(6)	    ,
	slettet_flagg	 number(1,0)	    not null,
	reaktivert_flagg number(1,0)	    not null,
	nyeste_status	 number(1,0)	    not null,
	kildesystem	     varchar2(10 byte)	,
	lastet_dato      timestamp(6)
)
;

grant select on dk_p.sbs_behandling_slettet to dvh_dk_p_ro_role;
grant insert on dk_p.sbs_behandling_slettet to dvh_dk_p_rw_role;
grant select on dk_p.sbs_behandling_slettet to dt_p with grant option;

create table dk_p.sbs_stage_behandling_hist_part (
	-- se fagsak for mer detaljer
	pk_sbs_behandling_historikk    number(38)    not null,
	lk_sbs_behandling_t  varchar2(38)  not null,
	lk_sbs_behandling    varchar2(38)  not null,
	lk_sbs_fagsak                  varchar2(38)  not null,
    lk_sbs_behandling_relatert     varchar2(38) ,
	lk_sbs_behandling_vedtak       varchar2(38) ,
	-- pseudonymous foreign key
	fk_utenlandstilsnitt_fin 	   number(38)	not null, 
	fk_sak_type	                   number(38)   not null,
	--fk_stonad                      number(38)   not null,
	fk_behandling_status           number(38)   not null,
	fk_sak_resultat                number(38)   not null,
	fk_ek_org_node                 number(38)   not null,
	sekvens						   number(38)   ,
	-- codes describing distributional factors according to the source
	behandling_kode                    varchar2(100), -- dette er da en ferdig "beregnede stønadskoden" for finne opphav til koden må forkammeret gi dette. 
	utenlandstilsnitt_kode		   varchar2(40), -- vanlig utenlandtilsnitt 
	sak_type_kode                  varchar2(100), -- behandlingstype 
	behandling_status_kode         varchar2(40), -- behandlingsstatus - her brukte vi behandlingssteg før, men dette bør utgå. 
    venter_utland_flagg            number(38)   ,    
	resultat_kode                  varchar2(40), -- behandlingsresultat 
	lk_org_enhet                   varchar2(40), -- organisasjonsenhetennummeret
	
	-- functional data extracted from the source
	avsluttet_flagg				   number(1) not null, -- flagget identifiserer om status koden indikerer at behandlingen er ferdig. 
	totrinn_flagg                  number(1) not null,  
	-- time related data for creation
	opprettet_trans_tid            timestamp(6)  not null,
	endret_av_kode                 varchar2(40) ,
	beslutter	varchar2(40) ,
	saksbehandler varchar2(40) ,
	funksjonell_tid                timestamp(6)  not null,
	mottatt_tid				       timestamp(6) ,
	--registrert_tid   			   timestamp(6) ,
    dato_for_uttak                 timestamp(6) ,
	-- meta data as implemented by the datawarehouse standard
	lastet_dato                    timestamp(6) not null,  
	lastet_session                 varchar(40)  not null,
	data_opphav					   varchar(40)  not null, 
	kildesystem                    varchar2(10) not null
) column store compress for query high
partition by range(funksjonell_tid) interval(numtoyminterval(1,'month'))
( partition p0 values less than ( to_date('20060101','yyyymmdd') ) )
;	

grant select on dk_p.sbs_stage_behandling_hist_part to dvh_vedlikehold_ro_role;
grant insert on dk_p.sbs_stage_behandling_hist_part to dvh_vedlikehold_rw_role;

create table dk_p.sbs_stage_behandling_hist (
	-- se fagsak for mer detaljer
	pk_sbs_behandling_historikk    number(38)    not null,
	--fk_sbs_fagsak                   number(38)    not null,
	lk_sbs_behandling_t  varchar2(38)  not null,
	lk_sbs_behandling    varchar2(38)  not null,
	lk_sbs_fagsak                  varchar2(38)  not null,
    lk_sbs_behandling_relatert     varchar2(38) ,
	lk_sbs_behandling_vedtak       varchar2(38) ,
	-- pseudonymous foreign key
	fk_utenlandstilsnitt_fin 	   number(38)	not null, -- her bør det kanskje avgjøres noe?
	fk_sak_type	                   number(38)   not null,
	--fk_stonad                      number(38)   not null,
	fk_behandling_status           number(38)   not null,
	fk_sak_resultat                number(38)   not null,
	fk_ek_org_node                 number(38)   not null,
	sekvens						   number(38)   ,
	-- codes describing distributional factors according to the source
	behandling_kode                    varchar2(100), -- dette er da en ferdig "beregnede stønadskoden" for å finne opphav til koden må forkammeret gi dette. 
	utenlandstilsnitt_kode		   varchar2(40), -- vanlig utenlandtilsnitt 
	sak_type_kode                  varchar2(100), -- behandlingstype 
	behandling_status_kode         varchar2(40), -- behandlingsstatus - her brukte vi behandlingssteg før men dette bør utgå. 
    venter_utland_flagg            number(38)   ,    
	resultat_kode                  varchar2(40), -- behandlingsresultat 
	lk_org_enhet                   varchar2(40), -- organisasjonsenhetennummeret
	
	-- functional data extracted from the source
	avsluttet_flagg				   number(1) not null, -- flagget identifiserer om status koden indikerer at behandlingen er ferdig. 
	totrinn_flagg                  number(1) not null,  
	-- time related data for creation
	opprettet_trans_tid            timestamp(6)  not null,
	endret_av_kode                 varchar2(40) ,
	beslutter	                   varchar2(40) ,
	saksbehandler                  varchar2(40) ,
	funksjonell_tid                timestamp(6)  not null,
	mottatt_tid				       timestamp(6) ,
	-- registrert_tid   			   timestamp(6) ,
    dato_for_uttak                 timestamp(6) ,
	-- meta data as implemented by the datawarehouse standard
	lastet_dato                    timestamp(6) not null,  
	lastet_session                 varchar(40)  not null,
	data_opphav					   varchar(40)  not null, 
	kildesystem                    varchar2(10) not null
) column store compress for query high;

grant select on dk_p.sbs_stage_behandling_hist to dk_p;
grant insert on dk_p.sbs_stage_behandling_hist to dk_p;
grant select on dk_p.sbs_stage_behandling_hist to dvh_vedlikehold_ro_role;
grant insert on dk_p.sbs_stage_behandling_hist to dvh_vedlikehold_rw_role;

create table dk_p.sbs_behandling_historikk_exch (
	-- se fagsak for mer detaljer
	pk_sbs_behandling_historikk    number(38)    not null,
	--fk_sbs_fagsak                   number(38)    not null,
	lk_sbs_behandling_t  varchar2(38)  not null,
	lk_sbs_behandling    varchar2(38)  not null,
	lk_sbs_fagsak                  varchar2(38)  not null,
    lk_sbs_behandling_relatert     varchar2(38) ,
	lk_sbs_behandling_vedtak       varchar2(38) ,
	-- pseudonymous foreign key
	fk_utenlandstilsnitt_fin 	   number(38)	not null, -- her bør det kanskje avgjøres noe?
	fk_sak_type	                   number(38)   not null,
	--fk_stonad                      number(38)   not null,
	fk_behandling_status           number(38)   not null,
	fk_sak_resultat                number(38)   not null,
	fk_ek_org_node                 number(38)   not null,
	sekvens						   number(38)   ,
	-- codes describing distributional factors according to the source
	behandling_kode                varchar2(100), -- dette er da en ferdig "beregnede stønadskoden" for å finne opphav til koden må forkammeret gi dette. 
	utenlandstilsnitt_kode		   varchar2(40), -- vanlig utenlandtilsnitt 
	sak_type_kode                  varchar2(100), -- behandlingstype 
	behandling_status_kode         varchar2(40), -- behandlingsstatus - her brukte vi behandlingssteg før men dette bør utgå. 
    venter_utland_flagg            number(38)   ,    
	resultat_kode                  varchar2(40), -- behandlingsresultat 
	lk_org_enhet                   varchar2(40), -- organisasjonsenhetennummeret
	-- functional data extracted from the source
	avsluttet_flagg				   number(1) not null, -- flagget identifiserer om status koden indikerer at behandlingen er ferdig. 
	totrinn_flagg                  number(1) not null,  
	-- time related data for creation
	opprettet_trans_tid            timestamp(6)  not null,
	endret_av_kode                 varchar2(40) ,
	beslutter	                   varchar2(40) ,
	saksbehandler                  varchar2(40) ,
	funksjonell_tid                timestamp(6)  not null,
	mottatt_tid				       timestamp(6) ,
	--registrert_tid   			   timestamp(6) ,
    dato_for_uttak                 timestamp(6) ,
	-- meta data as implemented by the datawarehouse standard
	lastet_dato                    timestamp(6) not null,  
	lastet_session                 varchar(40)  not null,
	data_opphav					   varchar(40)  not null, 
	kildesystem                    varchar2(10) not null
) column store compress for query high;

grant select on dk_p.sbs_behandling_historikk_exch to dvh_vedlikehold_ro_role;
grant insert on dk_p.sbs_behandling_historikk_exch to dvh_vedlikehold_rw_role;
  

create table dk_p.sbs_stage_behandling_hist_dl
column store compress for query high
partition by list(kildesystem)--Da kan vi enklere ha flere kildesystem i disse tabellene samtidig
( partition P_BISYS values ('BISYS'),
  partition P_INFOTRYGD values ('INFOTRYGD'),
  partition P_MELOSYS values ('MELOSYS'),
  partition P_FPSAK values ('FPSAK'))
as
select 
    h.*, 
    cast (null as number(38)) hash_key  
from dk_p.sbs_stage_behandling_hist h
where rownum=0
;




create table dk_p.sbs_stage_behandling_hist_fl
column store compress for query high
partition by list(kildesystem)--Da kan vi enklere ha flere kildesystem i disse tabellene samtidig
( partition P_BISYS values ('BISYS'),
  partition P_INFOTRYGD values ('INFOTRYGD'),
  partition P_MELOSYS values ('MELOSYS'),
  partition P_FPSAK values ('FPSAK'))
as
select 
    h.*, 
    cast (null as number(38)) hash_key  
from dk_p.sbs_stage_behandling_hist h
where rownum=0
;


grant select on dk_p.sbs_stage_behandling_hist_dl to dvh_vedlikehold_ro_role;
grant insert on dk_p.sbs_stage_behandling_hist_dl to dvh_vedlikehold_rw_role;

grant select on dk_p.sbs_stage_behandling_hist_fl to dvh_vedlikehold_ro_role;
grant insert on dk_p.sbs_stage_behandling_hist_fl to dvh_vedlikehold_rw_role;
