CREATE TABLE MON.SPARK_FT_CXD_NBRE_ACHATS_FORFAITS(
    numero varchar(200),
    nbre_transactions_om bigint,
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

---Staging table in DWH
CREATE TABLE MON.SQ_FT_CXD_NBRE_AT_FF (
    numero varchar(200),
    nbre_transactions_om NUMBER,
    nbre_achats_om  NUMBER,
    nbre_achats_forfaits_om NUMBER,
    nbre_echecs_achats_forfaits_om NUMBER,
    duree_min_depot_fortait float ,
    duree_moyenne_depot_fortait float,
    duree_mediane_depot_fortait float,
    duree_max_depot_fortait float,
    heure_min_echec_achat_forfait varchar(50),
    heure_moy_echec_achat_forfait varchar(50),
    heure_med_echec_achat_forfait varchar(50),
    heure_max_echec_achat_forfait varchar(50),
    insert_date DATE,
    EVENT_DATE DATE
);




---Staging table in data lake
CREATE TABLE TMP.SQ_FT_CXD_NBRE_AT_FF(
    numero varchar(200),
    nbre_transactions_om bigint,
    nbre_achats_om  bigint,
    nbre_achats_forfaits_om bigint,
    nbre_echecs_achats_forfaits_om bigint,
    duree_min_depot_fortait double ,
    duree_moyenne_depot_fortait double ,
    duree_mediane_depot_fortait double ,
    duree_max_depot_fortait double ,
    heure_min_echec_achat_forfait varchar(50),
    heure_moy_echec_achat_forfait varchar(50),
    heure_med_echec_achat_forfait varchar(50),
    heure_max_echec_achat_forfait varchar(50),
    insert_date timestamp,
    EVENT_DATE DATE
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_CXD_NBRE_AT_FF';
  MIN_DATE_PARTITION := '20220101';
  MAX_DATE_PARTITION := '20270101';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_CXD_NBRE_AT_FF';
  PART_PARTITION_NAME := 'FT_CXD_NBRE_AT_FF_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_MON_J17_256M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;
