CREATE TABLE AGG.FT_A_APPELS_CTI  (
mois varchar(10),
semaine varchar(10),
tranche_30min varchar(10),
AGENT_LOGIN VARCHAR(15),
AGENT_NOM VARCHAR(100),
short_number varchar(15),
site varchar(15),
appels_recus int,
appels_traites int,
appels_perdus_systeme int,
appels_perdus_sonnerie int,
appels_perdus_file int,
appels_perdus_attente int,
appels_valides int,
appel_invalides int,
total_appels int,
appels_pris_40s int,
appels_pris_60s int,
appels_pris_120s int,
duree_appels_recus int,
moyenne_appels_recus double,
duree_appels_traites int,
duree_appels_perdus_sonnerie int,
duree_appels_perdus_file int,
duree_appels_perdus_attente int,
duree_appels_valides int,
duree_appel_invalides int,
duree_totale int,
nombre_client int,
insert_date timestamp
)
PARTITIONED BY (EVENT_DATE DATE)
CLUSTERED BY(site,short_number) INTO 4 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;