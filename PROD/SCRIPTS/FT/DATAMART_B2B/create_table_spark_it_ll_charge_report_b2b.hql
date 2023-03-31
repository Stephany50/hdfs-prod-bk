CREATE TABLE CDR.SPARK_IT_LL_CHARGE_REPORT
(
    Account_Code VARCHAR(200),
    Customer_Name VARCHAR(200),
    Bill_Month VARCHAR(200),
    Capacity VARCHAR(200),
    Account_Status VARCHAR(200),
    Username VARCHAR(200),
    Link VARCHAR(200),
    Bill_Amount VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(100),
    INSERT_DATE TIMESTAMP,
    ORIGINAL_FILE_DATE  DATE
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_LL_CHARGE_REPORT
(
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    Account_Code VARCHAR(200),
    Customer_Name VARCHAR(200),
    Bill_Month VARCHAR(200),
    Capacity VARCHAR(200),
    Account_Status VARCHAR(200),
    Username VARCHAR(200),
    Link VARCHAR(200),
    Bill_Amount VARCHAR(200)
)COMMENT 'CDR SPARK_TT_LL_CHARGE_REPORT'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
LOCATION '/PROD/TT/STAT_TOOLS/DATAMARTB2B/BILL_LL_CHARGE/'
TBLPROPERTIES ('serialization.null.format'='');


---Staging table in DWH
CREATE TABLE MON.SQ_FT_LL_CHARGE_REPORT (
    Account_Code VARCHAR (100),
    Customer_Name VARCHAR (100),
    Bill_Month VARCHAR (100),
    Capacity VARCHAR (100),
    Account_Status VARCHAR (100),
    Username VARCHAR (100),
    Link VARCHAR (200),
    Bill_Amount VARCHAR (100),
    INSERT_DATE TIMESTAMP,
    EVENT_DATE DATE,
    EVENT_MONTH VARCHAR (255)
);




---Staging table in data lake
CREATE TABLE TMP.SQ_FT_LL_CHARGE_REPORT (
    Account_Code VARCHAR (100),
    Customer_Name VARCHAR (100),
    Bill_Month VARCHAR (100),
    Capacity VARCHAR (100),
    Account_Status VARCHAR (100),
    Username VARCHAR (100),
    Link VARCHAR (200),
    Bill_Amount VARCHAR (100),
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
  SAMPLE_TABLE := 'MON.SQ_FT_LL_CHARGE_REPORT';
  MIN_DATE_PARTITION := '20220101';
  MAX_DATE_PARTITION := '20270101';
  KEY_COLUMN_PART_NAME := 'EVENT_MONTH';
  KEY_COLUMN_PART_TYPE := 'MOIS';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'FT_LL_CHARGE_REPORT';
  PART_PARTITION_NAME := 'FT_LL_CHARGE_REPORT_';
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