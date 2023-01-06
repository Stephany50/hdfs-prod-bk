-- create table mon.spark_ft_cbm_quickeval 
create table tmp.spark_ft_cbm_quickeval 
  (
  msisdn varchar(50),
  temoin varchar(5),
  ipp1 varchar(50),
  ipp2 varchar(50),
  ipp3 varchar(50),
  problematique varchar(50),
  takers1 int,
  takers2 int,
  takers3 int,
  takers int, --max(taker1, taker2, taker3)
  nb_souscriptions_periode1 int,
  nb_souscriptions_periode2 int,
  ca_forfait_periode1 decimal(30, 5), -- chiffre d affaire genere sur la periode 1 venant de soit ipp1, ipp2 ou ipp3
  ca_forfait_periode2 decimal(30, 5),
  arpu_periode1 decimal(30, 5), -- arpu total sur toute les transanctions dans toutes la periode 1
  arpu_periode2 decimal(30, 5),
  arpu_data_periode1 decimal(30, 5),
  arpu_data_periode2 decimal(30, 5),
  arpu_voix_periode1 decimal(30, 5),
  arpu_voix_periode2 decimal(30, 5),
  volume_data_periode1 decimal(30, 5),
  volume_data_periode2 decimal(30, 5),
  mou_periode1 decimal(30, 5),
  mou_periode2 decimal(30, 5),
  activite_data int,
  activite_voix int,
  activite_globale int, --max(activite_data,activite_voix)
  evol_ca_forfait decimal(20, 5),
  evol_revenu decimal(20, 5),
  evol_data decimal(20, 5),
  evol_voix decimal(20, 5),
  evol_volume_data decimal(20, 5),
  evol_mou decimal(20, 5)
  )PARTITIONED BY(code_campagne varchar(50), event_time timestamp)
STORED AS PARQUET TBLPROPERTIES('PARQUET.COMPRESS'='SNAPPY')