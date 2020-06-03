whenever sqlerror exit failure;
alter session set current_schema=dt_p;
-- droper tabeller og seksvenser om de eksisterer fra før. 
set echo off
declare
   type varchar2_arr is varray(3) of varchar2(100);
   drop_table_arr varchar2_arr := varchar2_arr('drop table dt_p.fak_sbs_behandling_dag cascade constraints'
											  ,'drop table dt_p.fak_sbs_stage_beh_part cascade constraints'
                                              ,'drop table dt_p.fak_sbs_behandling_exch cascade constraints'
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

-- lager fakta tabell.
create table dt_p.fak_sbs_behandling_dag (
	fk_dim_tid_dag				number(38,0)		not null,
	fk_dim_tid_uke      		number(38,0)		not null,
	fk_dim_tid_mnd      		number(38,0)		not null,
	saksnummer					varchar2(100 byte)	        ,
	lk_sbs_behandling			varchar2(100 byte)	not null,
	lk_sbs_behandling_vedtak    varchar2(100 byte)			,
	lk_sbs_fagsak				varchar2(100 byte)	,
	fk_sbs_behandling    		number(38,0)    	not null,
	fk_sbs_fagsak				number(38,0)    	not null,
	fk_dim_f_sak_type			number(38,0)		not null,
	fk_dim_f_resultat			number(38,0)		not null,
	fk_dim_f_behandling_status	number(38,0)		not null,
	fk_dim_f_stonad_omraade		number(38,0)		not null,
	fk_dim_utenlandstilsnitt	number(38,0)		not null,
	fk_dim_kjonn				number(38,0)		not null,
	fk_dim_geografi_bosted		number(38,0)		not null,
	fk_dim_org_mottagende		number(38,0)		not null,
	fk_dim_org_produksjon		number(38,0)		not null,
	fk_dim_org_ansvarlig_naa	number(38,0)		not null,
	fk_person1              	number(38,0)		not null,
    venter_utland_flagg         number(38)          not null,    
	siste_i_uke_flagg		    number(1)    		not null,
	siste_i_maaned_flagg        number(1)    		not null,
	mottatt_flagg				number(1)			not null,
    inngang_flagg				number(1)			not null,
	avsluttet_flagg			    number(1)			not null,
	restanse_flagg				number(1)			not null,
	totrinn_flagg				number(1)       	not null,
    oversendt_flagg				number(1)       	not null,
    tilsendt_flagg				number(1)       	not null,
	stonad_kode                 varchar(200)         , 
    mottatt_tid                 timestamp(6)		        ,
	inngang_tid					timestamp(6)		not null,
    dato_for_uttak              timestamp(6) ,
	produsert_tid				timestamp(6)	            ,
	varighet_dager				number(38,0)		not null,
	varighet_organisasjon       number(38,0)		not null,
	beslutter	                varchar2(40) ,
	saksbehandler               varchar2(40) ,
	lastet_dato					timestamp(6)		not null,
    lastet_session              varchar(40)         not null,
	kildesystem					varchar2(100)	    not null
) column store compress for query high
partition by range (fk_dim_tid_dag) interval(100)
(partition p0 values less than (19000000))
;
create unique index pk_fak_sbs_behandling_dag on fak_sbs_behandling_dag (fk_dim_tid_dag, lk_sbs_behandling, kildesystem) local;

grant select on dt_p.fak_sbs_behandling_dag to dvh_dt_p_ro_role;
grant insert on dt_p.fak_sbs_behandling_dag to dvh_dt_p_rw_role;
grant select on dt_p.fak_sbs_behandling_dag to hpssmon_les;

create table dt_p.fak_sbs_stage_beh_part (
	fk_dim_tid_dag				number(38,0)		not null,
	fk_dim_tid_uke      		number(38,0)		not null,
	fk_dim_tid_mnd      		number(38,0)		not null,
    saksnummer					varchar2(100 byte)	        ,
	lk_sbs_behandling			varchar2(100 byte)	not null,
	lk_sbs_behandling_vedtak    	varchar2(100 byte)			,
	lk_sbs_fagsak				varchar2(100 byte)	,
	fk_sbs_behandling    		number(38,0)    	not null,
	fk_sbs_fagsak				number(38,0)    	not null,
	fk_dim_f_sak_type			number(38,0)		not null,
	fk_dim_f_resultat			number(38,0)		not null,
	fk_dim_f_behandling_status	number(38,0)		not null,
	fk_dim_f_stonad_omraade		number(38,0)		not null,
	fk_dim_utenlandstilsnitt	number(38,0)		not null,
	fk_dim_kjonn				number(38,0)		not null,
	fk_dim_geografi_bosted		number(38,0)		not null,
	fk_dim_org_mottagende		number(38,0)		not null,
	fk_dim_org_produksjon		number(38,0)		not null,
	fk_dim_org_ansvarlig_naa	number(38,0)		not null,
	fk_person1              	number(38,0)		not null,
    venter_utland_flagg         number(38)          not null,    
	siste_i_uke_flagg		    number(1)    		not null,
	siste_i_maaned_flagg        number(1)    		not null,
	mottatt_flagg				number(1)			not null,
    inngang_flagg				number(1)			not null,
	avsluttet_flagg         	number(1)			not null,
	restanse_flagg				number(1)			not null,
	totrinn_flagg				number(1)       	not null,
    oversendt_flagg				number(1)       	not null,
    tilsendt_flagg				number(1)       	not null,
	stonad_kode                 varchar(200)         , 
	mottatt_tid                 timestamp(6)		        ,
    inngang_tid					timestamp(6)		not null,
    dato_for_uttak              timestamp(6) ,
	produsert_tid				timestamp(6)	            ,
	varighet_dager				number(38,0)		not null,
	varighet_organisasjon       number(38,0)		not null,
	beslutter	varchar2(40) ,
	saksbehandler varchar2(40) ,
	lastet_dato					timestamp(6)		not null,
    lastet_session              varchar(40)         not null,
	kildesystem					varchar2(100)	    not null
) column store compress for query high
partition by range (fk_dim_tid_dag) interval(100)
(partition p0 values less than (19000000))
;
create unique index pk_fak_sbs_stage_beh_part on fak_sbs_stage_beh_part (fk_dim_tid_dag, lk_sbs_behandling, kildesystem) local;

grant select on dt_p.fak_sbs_stage_beh_part to dvh_vedlikehold_ro_role;
grant insert on dt_p.fak_sbs_stage_beh_part to dvh_vedlikehold_rw_role;
grant select on dt_p.fak_sbs_stage_beh_part to dvh_dt_p_ro_role;--trenger denne for at pl/sql-pakke i DK_P skal kunne kjøre dbms_stats

create table dt_p.fak_sbs_behandling_exch (
	fk_dim_tid_dag				number(38,0)		not null,
	fk_dim_tid_uke      		number(38,0)		not null,
	fk_dim_tid_mnd      		number(38,0)		not null,
	saksnummer					varchar2(100 byte)	        ,
	lk_sbs_behandling			varchar2(100 byte)	not null,
	lk_sbs_behandling_vedtak    	varchar2(100 byte)			,
	lk_sbs_fagsak				varchar2(100 byte)	,
	fk_sbs_behandling    		number(38,0)    	not null,
	fk_sbs_fagsak				number(38,0)    	not null,
	fk_dim_f_sak_type			number(38,0)		not null,
	fk_dim_f_resultat			number(38,0)		not null,
	fk_dim_f_behandling_status	number(38,0)		not null,
	fk_dim_f_stonad_omraade		number(38,0)		not null,
	fk_dim_utenlandstilsnitt	number(38,0)		not null,
	fk_dim_kjonn				number(38,0)		not null,
	fk_dim_geografi_bosted		number(38,0)		not null,
	fk_dim_org_mottagende		number(38,0)		not null,
	fk_dim_org_produksjon		number(38,0)		not null,
	fk_dim_org_ansvarlig_naa	number(38,0)		not null,
	fk_person1              	number(38,0)		not null,
    venter_utland_flagg         number(38)          not null,    
	siste_i_uke_flagg		    number(1)    		not null,
	siste_i_maaned_flagg        number(1)    		not null,
	mottatt_flagg				number(1)			not null,
    inngang_flagg				number(1)			not null,
	produksjon_flagg			number(1)			not null,
	restanse_flagg				number(1)			not null,
	totrinn_flagg				number(1)       	not null,
    oversendt_flagg				number(1)       	not null,
    tilsendt_flagg				number(1)       	not null,
	stonad_kode                 varchar(200)         , 
	mottatt_tid                 timestamp(6)		        ,
    inngang_tid					timestamp(6)		not null,
    dato_for_uttak              timestamp(6) ,
	produsert_tid				timestamp(6)	            ,
	varighet_dager				number(38,0)		not null,
	varighet_organisasjon       number(38,0)		not null,
	beslutter	                varchar2(40) ,
	saksbehandler               varchar2(40) ,
	lastet_dato					timestamp(6)		not null,
    lastet_session              varchar(40)         not null,
	kildesystem					varchar2(100)	    not null
) column store compress for query high
;
create unique index pk_fak_sbs_behandling_exch on fak_sbs_behandling_exch (fk_dim_tid_dag, lk_sbs_behandling, kildesystem);

grant select on dt_p.fak_sbs_behandling_exch to dvh_vedlikehold_ro_role;
grant insert on dt_p.fak_sbs_behandling_exch to dvh_vedlikehold_rw_role;

alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_f_sak_type_exch		  foreign key (fk_dim_f_sak_type		 ) references dt_p.dim_f_sak_type          rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_f_resultat_exch		  foreign key (fk_dim_f_resultat		 ) references dt_p.dim_f_resultat          rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_f_beh_status_exch      foreign key (fk_dim_f_behandling_status) references dt_p.dim_f_behandling_status rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_f_stonad_omraade_exch  foreign key (fk_dim_f_stonad_omraade	 ) references dt_p.dim_f_stonad_omraade    rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_utenlandstilsnitt_exch foreign key (fk_dim_utenlandstilsnitt  ) references dt_p.dim_utenlandstilsnitt   rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_geografi_bosted_exch	  foreign key (fk_dim_geografi_bosted	 ) references dt_p.dim_geografi            rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_tid_dag_exch           foreign key (fk_dim_tid_dag			 ) references dt_p.dim_tid	              rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_tid_uke_exch           foreign key (fk_dim_tid_uke        	 ) references dt_p.dim_tid	              rely disable novalidate;
alter table dt_p.fak_sbs_behandling_exch add constraint fk_sbs_tid_mnd_exch           foreign key (fk_dim_tid_mnd    		 ) references dt_p.dim_tid	              rely disable novalidate;

alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_f_sak_type		  foreign key (fk_dim_f_sak_type		 ) references dt_p.dim_f_sak_type          rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_f_resultat		  foreign key (fk_dim_f_resultat		 ) references dt_p.dim_f_resultat          rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_f_behandling_status foreign key (fk_dim_f_behandling_status) references dt_p.dim_f_behandling_status rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_f_stonad_omraade	  foreign key (fk_dim_f_stonad_omraade	 ) references dt_p.dim_f_stonad_omraade    rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_utenlandstilsnitt	  foreign key (fk_dim_utenlandstilsnitt  ) references dt_p.dim_utenlandstilsnitt   rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_geografi_bosted	  foreign key (fk_dim_geografi_bosted	 ) references dt_p.dim_geografi            rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_tid_sbs_beh_dag     foreign key (fk_dim_tid_dag			 ) references dt_p.dim_tid	               rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_tid_sbs_beh_uke     foreign key (fk_dim_tid_uke		     ) references dt_p.dim_tid	               rely disable novalidate;
alter table dt_p.fak_sbs_behandling_dag add constraint fk_sbs_tid_sbs_beh_mnd     foreign key (fk_dim_tid_mnd    	     ) references dt_p.dim_tid	               rely disable novalidate;
