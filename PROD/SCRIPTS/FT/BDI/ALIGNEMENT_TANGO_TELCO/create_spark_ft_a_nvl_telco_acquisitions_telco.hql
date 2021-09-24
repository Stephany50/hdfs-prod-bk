--- BY BornToBe
--- NAME : AGG.SPARK_FT_A_NVL_ACQUISITIONS_TELCO
--- DESC : Contient des KPI nous permettant d'avoir l'etat de l'alignement Telco/OM des nouvelles acquisitions Telco.
---    
---FIELDS:
---      * Les attributs préfixés par 'inter_'

CREATE TABLE AGG.SPARK_FT_A_NVL_ACQUISITIONS_TELCO(
  acq_total bigint,
  acq_valide_bot bigint,
  acq_non_valide_bot bigint,
  inter_total bigint,
  inter_valide_bom bigint,
  inter_non_valide_bom bigint,
  inter_recycle bigint,
  inter_non_recycle bigint,
  inter_nom bigint,
  inter_naiss bigint,
  inter_piece bigint,
  inter_all bigint,
  insert_date timestamp
)comment 'SPARK_FT_A_NVL_ACQUISITIONS_TELCO table'
partitioned by (event_date date)
stored AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


--Staging table in DataLake
CREATE TABLE TMP.SQ_FT_A_NVL_ACQUISITIONS_TELCO(
    acq_total decimal(20,3),
    acq_valide_bot decimal(20,3),
    acq_non_valide_bot decimal(20,3),
    inter_total decimal(20,3),
    inter_valide_bom decimal(20,3),
    inter_non_valide_bom decimal(20,3),
    inter_recycle decimal(20,3),
    inter_non_recycle decimal(20,3),
    inter_nom decimal(20,3),
    inter_naiss decimal(20,3),
    inter_piece decimal(20,3),
    inter_all decimal(20,3),
    insert_date timestamp,
    event_date date
);


--Staging table in DWH
CREATE TABLE MON.SQ_FT_A_NVL_ACQUISITIONS_TELCO
(
    acq_total decimal(20,3),
    acq_valide_bot decimal(20,3),
    acq_non_valide_bot decimal(20,3),
    inter_total decimal(20,3),
    inter_valide_bom decimal(20,3),
    inter_non_valide_bom decimal(20,3),
    inter_recycle decimal(20,3),
    inter_non_recycle decimal(20,3),
    inter_nom decimal(20,3),
    inter_naiss decimal(20,3),
    inter_piece decimal(20,3),
    inter_all decimal(20,3),
    insert_date timestamp,
    event_date date
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_A_NVL_ACQUISITIONS_TELCO';
  MIN_DATE_PARTITION := '20210801';
  MAX_DATE_PARTITION := '20220801';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_A_NVL_ACQUISITIONS_TELCO';
  PART_PARTITION_NAME := 'FT_A_NVL_ACQ_';
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

