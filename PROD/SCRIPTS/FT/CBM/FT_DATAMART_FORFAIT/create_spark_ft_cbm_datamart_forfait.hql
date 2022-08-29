CREATE TABLE MON.SPARK_FT_CBM_DATAMART_FORFAIT(
bdle_name varchar(255),
id_bdle_name int,
insert_date date,
subscription_channel varchar(255),
souscriptions bigint,
revenu DECIMAL(17,2),
Paiement varchar(50),
type varchar(50),
coef_data int,
coef_voix int,
CA_data DECIMAL(17,2),
CA_voix DECIMAL(17,2)
)comment 'SPARK_FT_CBM_DATAMART_FORFAIT table'
partitioned by (event_date date)
stored AS parquet TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');



-- DECLARE 
--   SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
--   KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
--   PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
-- PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
-- BEGIN 
--   SAMPLE_TABLE := 'MON.SQ_FT_A_KYC_DASH_KPIS_TELCO';
--   MIN_DATE_PARTITION := '20220101';
--   MAX_DATE_PARTITION := '20230101';
--   KEY_COLUMN_PART_NAME := 'EVENT_DATE';
--   KEY_COLUMN_PART_TYPE := 'JOUR';
--   PART_OWNER := 'MON';
--   PART_TABLE_NAME := 'FT_A_KYC_DASH_KPIS_TELCO';
--   PART_PARTITION_NAME := 'FT_KYC_DASH_KT_';
--   PART_TYPE_PERIODE := 'JOUR';
--   PART_RETENTION := 1000;
--   PART_TBS_CIBLE :=  'TAB_P_MON_Jour_16M';
--   PART_GARDER_01_DU_MOIS := 'NON';
--   PART_PCT_FREE := 0;
--   PART_COMPRESSION := 'COMPRESS';
--   PART_ROTATION_ACTIVE := 'OUI';
--   PART_FORMAT := 'yyyymmdd';
--   MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
--   COMMIT; 
-- END;
