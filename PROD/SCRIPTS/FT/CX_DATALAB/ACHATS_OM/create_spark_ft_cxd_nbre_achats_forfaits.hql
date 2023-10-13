CREATE TABLE MON.SPARK_FT_CXD_NBRE_ACHATS_FORFAITS(
    numero varchar(200),
    nbre_transanctions_om bigint,
    nbre_achats_om  bigint,
    nbre_achats_forfaits_om bigint,
    nbre_echecs_achats_forfaits_om bigint,
    duree_min_depot_fortait double ,
    duree_moyenne_depot_fortait double ,
    duree_mediane_depot_fortait double ,
    duree_max_depot_fortait double ,
    heure_min_echec_achat_forfait varchar(50),
    heure_moyenne_echec_achat_forfait varchar(50),
    heure_mediane_echec_achat_forfait varchar(50),
    heure_max_echec_achat_forfait varchar(50),
    insert_date timestamp
)PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

