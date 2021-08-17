--- BY BornToBe
--- NAME : AGG.SPARK_FT_A_KPI_TANGO_TELCO
--- DESC : Contient un ensemble de kpi permettant d'evaluer l'alignement des bases telco et OM.
---       
---FIELDS:
---      * kpi1 Effectif base.
---      * kpi2 : NOM_PRENOM_OM = NOM_PRENOM_TELCO
---      * kpi3 : NUMERO_PIECE_OM = NUMERO_PIECE_TELCO.
---      * kpi4 : DATE_NAISSANCE_OM = DATE_NAISSANCE_TELCO.
---      * kpi5 : (NOM_PRENOM_OM = NOM_PRENOM_TELCO) et (NUMERO_PIECE_OM = NUMERO_PIECE_TELCO).
---      * kpi6 : (NOM_PRENOM_OM = NOM_PRENOM_TELCO) et (NUMERO_PIECE_OM = NUMERO_PIECE_TELCO) et (DATE_NAISSANCE_OM = DATE_NAISSANCE_TELCO).
---      * kpi7 : (NOM_PRENOM_OM = NOM_PRENOM_TELCO) et (NUMERO_PIECE_OM = NUMERO_PIECE_TELCO sur les 9 premiers caractères)
---      * kpi8 : (NOM_PRENOM_OM = NOM_PRENOM_TELCO) et (NUMERO_PIECE_OM = NUMERO_PIECE_TELCO sur les 9 premiers caractères) et (DATE_NAISSANCE_OM = DATE_NAISSANCE_TELCO).
---      * kpi9 : (NOM_PRENOM_OM similaire à  NOM_PRENOM_TELCO) et (NUMERO_PIECE_OM = NUMERO_PIECE_TELCO) et (DATE_NAISSANCE_OM = DATE_NAISSANCE_TELCO).
---      * kpi10: (NOM_PRENOM_OM similaire à NOM_PRENOM_TELCO) et (NUMERO_PIECE_OM = NUMERO_PIECE_TELCO sur les 9 premiers caractères) et (DATE_NAISSANCE_OM = DATE_NAISSANCE_TELCO).

CREATE TABLE AGG.SPARK_FT_A_KPI_TANGO_TELCO(
  kpi1 bigint,
  kpi2 bigint,
  kpi3 bigint,
  kpi4 bigint,
  kpi5 bigint,
  kpi6 bigint,
  kpi7 bigint,
  kpi8 bigint,
  kpi9 bigint,
  kpi10 bigint,
  insert_date timestamp
)comment 'SPARK_FT_A_KPI_TANGO_TELCO table'
partitioned by (event_date date)
stored AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


--Staging table in DataLake
CREATE TABLE TMP.SQ_FT_A_KPI_TANGO_TELCO(
    kpi1            decimal(20,3),
    kpi2            decimal(20,3),
    kpi3            decimal(20,3),
    kpi4            decimal(20,3),
    kpi5            decimal(20,3),
    kpi6            decimal(20,3),
    kpi7            decimal(20,3),
    kpi8            decimal(20,3),
    kpi9            decimal(20,3),
    kpi10            decimal(20,3),
    insert_date timestamp,
    event_date date
);


--Staging table in DWH
CREATE TABLE MON.SQ_FT_A_KPI_TANGO_TELCO
(
  kpi1            decimal(20,3),
    kpi2            decimal(20,3),
    kpi3            decimal(20,3),
    kpi4            decimal(20,3),
    kpi5            decimal(20,3),
    kpi6            decimal(20,3),
    kpi7            decimal(20,3),
    kpi8            decimal(20,3),
    kpi9            decimal(20,3),
    kpi10            decimal(20,3),
    insert_date timestamp,
    event_date date
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_A_KPI_TANGO_TELCO';
  MIN_DATE_PARTITION := '20210801';
  MAX_DATE_PARTITION := '20220801';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_A_KPI_TANGO_TELCO';
  PART_PARTITION_NAME := 'FT_A_KPI_TT_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_MON_Jour_16M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;

