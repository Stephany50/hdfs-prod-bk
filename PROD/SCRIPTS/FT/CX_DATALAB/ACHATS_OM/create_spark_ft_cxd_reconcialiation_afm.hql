CREATE TABLE MON.SPARK_FT_CXD_RECONCIALIATION_AFM(
    numero varchar(200),
    transfer_id varchar(200),
    date_debit timestamp,
    date_depot timestamp,
    insert_date timestamp
)PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


flux.sqoop.export-rdms.staging-table = "MON.SQ_FT_CXD_RECON_AFM "
flux.sqoop.export-rdms.dest-table = "MON.FT_CXD_RECON_AFM "
flux.sqoop.export-hive.staging-table = "SQ_FT_CXD_RECON_AFM "
flux.sqoop.export-hive.staging-table-database = "TMP"

---Staging table in DWH
CREATE TABLE MON.SQ_FT_CXD_RECON_AFM(
    numero varchar(200),
    transfer_id varchar(200),
    date_debit timestamp,
    date_depot timestamp,
    insert_date DATE,
    EVENT_DATE DATE
);




---Staging table in data lake
CREATE TABLE TMP.SQ_FT_CXD_RECON_AFM(
    numero varchar(200),
    transfer_id varchar(200),
    date_debit timestamp,
    date_depot timestamp,
    insert_date timestamp
    EVENT_DATE DATE
);


DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_CXD_RECON_AFM';
  MIN_DATE_PARTITION := '20220101';
  MAX_DATE_PARTITION := '20270101';
  KEY_COLUMN_PART_NAME := 'EVENT_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_CXD_RECON_AFM';
  PART_PARTITION_NAME := 'FT_CXD_RECON_AFM_';
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
