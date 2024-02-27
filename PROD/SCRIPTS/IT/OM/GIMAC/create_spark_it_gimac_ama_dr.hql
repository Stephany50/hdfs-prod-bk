CREATE EXTERNAL TABLE CDR.SPARK_TT_GIMAC_AMA_DR (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ISSUER_REF varchar(100),
    ACQUIRER_REF  varchar(100),
    WALLET_SOURCE  varchar(100),
    WALLET_DESTINATION  varchar(100),
    AMOUNT  varchar(100),
    STATUS varchar(100),
    ERROR_MESSAGE  varchar(100),
    TRANSFER_DATETIME varchar(100),
    ACCOUNTING_STATUS  varchar(100),
    CHARGEBACK_DATE varchar(100),
    TYPE varchar(100)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/AMA_DR'
TBLPROPERTIES ('serialization.null.format'='')
;

CREATE TABLE CDR.SPARK_IT_GIMAC_AMA_DR
(
    ISSUER_REF varchar(100),
    ACQUIRER_REF  varchar(100),
    WALLET_SOURCE  varchar(100),
    WALLET_DESTINATION  varchar(100),
    AMOUNT  varchar(100),
    STATUS varchar(100),
    ERROR_MESSAGE  varchar(100),
    TRANSFER_DATETIME varchar(100),
    ACCOUNTING_STATUS  varchar(100),
    CHARGEBACK_DATE varchar(100),
    TYPE varchar(100),
    ORIGINAL_FILE_NAME  VARCHAR(100),
    ORIGINAL_FILE_SIZE  INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
   )
  PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");


  --Staging table in DWH
CREATE TABLE MON.SQ_IT_GIMAC_AMA_DR (
    ISSUER_REF varchar(100),
    ACQUIRER_REF  varchar(100),
    WALLET_SOURCE  varchar(100),
    WALLET_DESTINATION  varchar(100),
    AMOUNT  varchar(100),
    STATUS varchar(100),
    ERROR_MESSAGE  varchar(100),
    TRANSFER_DATETIME DATE,
    ACCOUNTING_STATUS  varchar(100),
    CHARGEBACK_DATE varchar(100),
    TYPE varchar(100),
    ORIGINAL_FILE_NAME  VARCHAR(100),
    INSERT_DATE TIMESTAMP,
    ORIGINAL_FILE_DATE DATE
);



---Staging table in data lake
CREATE TABLE TMP.SQ_IT_GIMAC_AMA_DR (
    ISSUER_REF varchar(100),
    ACQUIRER_REF  varchar(100),
    WALLET_SOURCE  varchar(100),
    WALLET_DESTINATION  varchar(100),
    AMOUNT  varchar(100),
    STATUS varchar(100),
    ERROR_MESSAGE  varchar(100),
    TRANSFER_DATETIME TIMESTAMP,
    ACCOUNTING_STATUS  varchar(100),
    CHARGEBACK_DATE varchar(100),
    TYPE varchar(100),
    ORIGINAL_FILE_NAME  VARCHAR(100),
    INSERT_DATE TIMESTAMP,
    ORIGINAL_FILE_DATE DATE
);



--"MON.IT_GIMAC_AMA_DR"

DECLARE 
  SAMPLE_TABLE VARCHAR2(200); MIN_DATE_PARTITION VARCHAR2(200); MAX_DATE_PARTITION VARCHAR2(200);  KEY_COLUMN_PART_NAME VARCHAR2(200);
  KEY_COLUMN_PART_TYPE VARCHAR2(200);   PART_OWNER VARCHAR2(200);  PART_TABLE_NAME VARCHAR2(200);  PART_PARTITION_NAME VARCHAR2(200);
  PART_TYPE_PERIODE VARCHAR2(200);  PART_RETENTION NUMBER;  PART_TBS_CIBLE VARCHAR2(200);  PART_GARDER_01_DU_MOIS VARCHAR2(200);
PART_PCT_FREE NUMBER;   PART_COMPRESSION VARCHAR2(200);  PART_ROTATION_ACTIVE VARCHAR2(200);  PART_FORMAT VARCHAR2(200);
BEGIN 
  SAMPLE_TABLE := 'MON.SQ_IT_GIMAC_AMA_DR';
  MIN_DATE_PARTITION := '20220101';
  MAX_DATE_PARTITION := '20220102';
  KEY_COLUMN_PART_NAME := 'ORIGINAL_FILE_DATE';
  KEY_COLUMN_PART_TYPE := 'JOUR';
  PART_OWNER := 'MON';
  PART_TABLE_NAME := 'IT_GIMAC_AMA_DR';
  PART_PARTITION_NAME := 'IT_GIMAC_AMA_DR_';
  PART_TYPE_PERIODE := 'JOUR';
  PART_RETENTION := 1000;
  PART_TBS_CIBLE :=  'TAB_P_CDR_J01_16M';
  PART_GARDER_01_DU_MOIS := 'NON';
  PART_PCT_FREE := 0;
  PART_COMPRESSION := 'COMPRESS';
  PART_ROTATION_ACTIVE := 'OUI';
  PART_FORMAT := 'yyyymmdd';
  MON.CREATE_PARTITIONED_TABLE ( SAMPLE_TABLE, MIN_DATE_PARTITION, MAX_DATE_PARTITION, KEY_COLUMN_PART_NAME, KEY_COLUMN_PART_TYPE, PART_OWNER, PART_TABLE_NAME, PART_PARTITION_NAME, PART_TYPE_PERIODE, PART_RETENTION, PART_TBS_CIBLE, PART_GARDER_01_DU_MOIS, PART_PCT_FREE, PART_COMPRESSION, PART_ROTATION_ACTIVE, PART_FORMAT );
  COMMIT; 
END;