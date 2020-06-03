whenever sqlerror exit failure;
-- droper tabeller og seksvenser om de eksisterer fra før. 
set echo off
declare
   type varchar2_arr is varray(6) of varchar2(100);
   drop_table_arr varchar2_arr := varchar2_arr('drop table dk_p.sbs_fagsak cascade constraints purge'
											  ,'drop table dk_p.sbs_behandling cascade constraints purge'
                                              ,'drop sequence dk_p.seq_sbs_fagsak'
                                              ,'drop sequence dk_p.seq_sbs_behandling'
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

create sequence dk_p.seq_sbs_fagsak cache 100;
create sequence dk_p.seq_sbs_behandling cache 100;
-- Tabell som inneholder behandlingshode

create table dk_p.sbs_fagsak(
 pk_sbs_fagsak number(38) not null
,lk_sbs_fagsak_t varchar2(40) not null 
,lk_sbs_fagsak varchar2(40) not null
,lk_aktoer varchar2(20)
,fk_person1 number(38) not null
,fagsak_kode varchar(40)
,fagsak_under_kode varchar(40)
,status_kode varchar(40)
,saksnummer_kode varchar(40)
,opprettet_trans_tid timestamp(6)
,inngang_tid timestamp(6)
,funksjonell_tid timestamp(6)
,data_opphav varchar(40) not null
,lastet_session varchar(40) not null
,lastet_dato timestamp(6)
,oppdatert_dato timestamp(6)
,kildesystem varchar(40) not null
) column store compress for query high
partition by list (kildesystem) 
     (
     partition fpsak
        values ('FPSAK'),
	 partition melosys
        values ('MELOSYS'),
	 partition infotrygd
        values ('INFOTRYGD'),
	 partition other
        values (default)
	)

    ;
create unique index dk_p.pk_sbs_fagsak on dk_p.sbs_fagsak (lk_sbs_fagsak, kildesystem) local;
alter table dk_p.sbs_fagsak add constraint pk_sbs_fagsak primary key (lk_sbs_fagsak, kildesystem) rely using index dk_p.pk_sbs_fagsak;

grant select on dk_p.sbs_fagsak to dvh_dk_p_ro_role;
grant insert, update, delete on dk_p.sbs_fagsak to dvh_dk_p_rw_role;
-- Tabell som inneholder behandling 

create table dk_p.sbs_behandling( 
 pk_sbs_behandling number(38) not null
,lk_sbs_fagsak varchar(40)
,lk_sbs_behandling_t varchar(40)
,lk_sbs_behandling varchar(40)
,lk_sbs_behandling_relatert varchar(40)
,lk_sbs_behandling_vedtak varchar(40)
,lk_org_enhet_inngang varchar(40)
,lk_org_enhet_avsluttet varchar(40)
,lk_org_enhet_naavarende varchar2(40)
,fk_ek_org_node_inngang number(38) not null
,fk_ek_org_node_avsluttet number(38) not null
,fk_ek_org_node_naavarende number(38)
,fk_sbs_fagsak number(38) not null
,fk_behandling_status number(38) not null
,fk_sak_resultat number(38) not null
,fk_sak_type number(38) not null
,fk_utenlandstilsnitt_fin number(38) not null
,behandling_kode varchar(100) 
,behandling_status_kode varchar(40) 
,resultat_kode varchar(40) 
,sak_type_kode varchar(100) 
,opprettet_trans_tid timestamp(6)
,funksjonell_tid timestamp(6)
,inngang_tid timestamp(6)
,mottatt_tid timestamp(6)
,registrert_tid timestamp(6)
,dato_for_uttak timestamp(6)
,innstilt_klage_tid timestamp(6)
,innstilt_vedtak_tid timestamp(6)
,avsluttet_tid timestamp(6)
,inngang_saksbehandler varchar(40) 
,avsluttet_saksbehandler varchar(40) 
,avsluttet_beslutter varchar(40) 
,avsluttet_flagg number(1) not null
,totrinn_flagg number(1) not null
,slettet_flagg number(1) not null
,data_opphav varchar(40) not null
,lastet_session varchar(40) not null
,lastet_dato timestamp(6)
,oppdatert_dato timestamp(6)
,kildesystem varchar(40) not null
) column store compress for query high
partition by list (kildesystem) 
     (
     partition fpsak
        values ('FPSAK'),
	 partition melosys
        values ('MELOSYS'),
	 partition infotrygd
        values ('INFOTRYGD'),
	 partition other
        values (default)
	)
;

create unique index dk_p.pk_sbs_behandling on dk_p.sbs_behandling (lk_sbs_behandling, kildesystem) local;
alter table dk_p.sbs_behandling add constraint pk_sbs_behandling primary key (lk_sbs_behandling, kildesystem) rely using index dk_p.pk_sbs_behandling;

grant select on dk_p.sbs_behandling to dvh_dk_p_ro_role;
grant insert, update, delete on dk_p.sbs_behandling to dvh_dk_p_rw_role;


alter table dk_p.sbs_behandling  
add constraint fk_sbs_fagsak_behandling			 
foreign key (lk_sbs_fagsak, kildesystem) 
references dk_p.sbs_fagsak (lk_sbs_fagsak, kildesystem) 
rely disable novalidate;

alter table dk_p.sbs_behandling_historikk 
add constraint fk_sbs_behandling
foreign key (lk_sbs_behandling, kildesystem) 
references dk_p.sbs_behandling (lk_sbs_behandling, kildesystem) 
rely disable novalidate;

alter table dk_p.sbs_fagsak_historikk 
add constraint fk_sbs_fagsak
foreign key (lk_sbs_fagsak, kildesystem) 
references dk_p.sbs_fagsak (lk_sbs_fagsak, kildesystem) 
rely disable novalidate;
