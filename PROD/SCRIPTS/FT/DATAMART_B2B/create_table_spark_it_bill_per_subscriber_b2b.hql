CREATE TABLE CDR.SPARK_IT_BILL_PER_SUBSCRIBER
(
    ACCT_NBR VARCHAR(200),
    ACCT_NAME VARCHAR(200),
    ACC_NBR VARCHAR(200),
    SUBS_PLAN VARCHAR(200),
    BILL_NBR VARCHAR(200),
    PREVIOUS_SUBS_CHARGE VARCHAR(200),
    SUBS_CHARGE VARCHAR(200),
    VARIATION VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(200),
    INSERT_DATE TIMESTAMP,
    ORIGINAL_FILE_DATE  DATE

)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_BILL_PER_SUBSCRIBER
(
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ACCT_NBR VARCHAR(200),
    ACCT_NAME VARCHAR(200),
    ACC_NBR VARCHAR(200),
    PREVIOUS_SUBS_CHARGE VARCHAR(200),
    SUBS_PLAN VARCHAR(200),
    BILL_NBR VARCHAR(200),
    SUBS_CHARGE VARCHAR(200),
    VARIATION VARCHAR(200)

)COMMENT 'CDR SPARK_TT_BILL_PER_SUBSCRIBER'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "\;"
)
LOCATION '/PROD/TT/STAT_TOOLS/DATAMARTB2B/BILL_PER_SUBSCRIBER'
TBLPROPERTIES ('serialization.null.format'='');

flux.sqoop.export-rdms.staging-table = "MON.SQ_FT_BILL_PER_SUBSCRIBER_MONTH"
flux.sqoop.export-rdms.dest-table = "MON.FT_BILL_PER_SUBSCRIBER"
flux.sqoop.export-hive.staging-table = "SQ_FT_BILL_PER_SUBSCRIBER_MONTH"



---Staging table in DWH
CREATE TABLE MON.SQ_FT_BILL_PER_SUBSCRIBER (
    ACCT_NBR VARCHAR (255),
    ACCT_NAME VARCHAR (255),
    ACC_NBR VARCHAR (255),
    SUBS_PLAN VARCHAR (255),
    BILL_NBR VARCHAR (255),
    PREVIOUS_SUBS_CHARGE FLOAT,
    SUBS_CHARGE FLOAT,
    VARIATION VARCHAR (255),
    INSERT_DATE TIMESTAMP,
    EVENT_DATE DATE
    EVENT_MONTH VARCHAR (255)
);




---Staging table in data lake
CREATE TABLE TMP.SQ_FT_BILL_PER_SUBSCRIBER_MONTH (
    ACCT_NBR VARCHAR (255),
    ACCT_NAME VARCHAR (255),
    ACC_NBR VARCHAR (255),
    SUBS_PLAN VARCHAR (255),
    BILL_NBR VARCHAR (255),
    PREVIOUS_SUBS_CHARGE FLOAT,
    SUBS_CHARGE FLOAT,
    VARIATION VARCHAR (255),
    INSERT_DATE TIMESTAMP,
    EVENT_DATE DATE,
    EVENT_MONTH VARCHAR (255)
);




DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_FT_BILL_PER_SUBSCRIBER';
  MIN_DATE_PARTITION := '20220101';
  MAX_DATE_PARTITION := '20270101';
  KEY_COLUMN_PART_NAME := 'EVENT_MONTH';
  KEY_COLUMN_PART_TYPE := 'MOIS';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_BILL_PER_SUBSCRIBER';
  PART_PARTITION_NAME := 'FT_BILL_PER_SUBSCRIBER_';
  PART_TYPE_PERIODE := 'MOIS';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_MON_J12_256M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymm';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;
