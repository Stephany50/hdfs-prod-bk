CREATE TABLE CDR.SPARK_IT_ZEMBLAREPORT (
  ID int,
  MSISDN varchar(15),
  SEGMENT int,
  SUBS_DATE varchar(200),
  IPP_CODE VARCHAR(100),
  PRICE DECIMAL(15,5),
  SUGGESTION VARCHAR(100),
  ORIGINAL_FILE_NAME VARCHAR(100),
  INSERT_DATE TIMESTAMP
)PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_ZEMBLAREPORT (
  ORIGINAL_FILE_NAME VARCHAR(100),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ID int,
  MSISDN varchar(15),
  SEGMENT int,
  SUBS_DATE timestamp,
  IPP_CODE VARCHAR(100),
  PRICE DECIMAL(15,5),
  SUGGESTION VARCHAR(100)
)COMMENT 'CDR SPARK_TT_ZEMBLAREPORT external table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
LOCATION '/PROD/TT/PERCO_Q1_2021/ZEMBLAREPORT'
TBLPROPERTIES ('serialization.null.format'='');






CREATE EXTERNAL TABLE CDR.spark_bundles_perco_q1_2021 (
  lot bigint,
  offer_code bigint,
  offer_name varchar(1000),
  type_bonus varchar(1000),
  comments varchar(1000)
)COMMENT 'CDR SPARK_TT_ZEMBLAREPORT external table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
LOCATION '/PROD/TT/PERCO_Q1_2021/ZEMBLAREPORT'
TBLPROPERTIES ('serialization.null.format'='');