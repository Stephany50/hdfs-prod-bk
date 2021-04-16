CREATE TABLE CDR.SPARK_IT_CHALLENGE_CLIENTS_PULSE (
  ID int,
  MSISDN varchar(15),
  DATE_MKT varchar(32),
  HEURE_MKT varchar(32),
  ORIGINAL_FILE_NAME VARCHAR(100),
  MODIFIED DATE,
  MB_CHALLENGE_USERS_ID int,
  INSERT_DATE TIMESTAMP
)PARTITIONED BY (CREATED DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_CHALLENGE_CLIENTS_PULSE (
  ORIGINAL_FILE_NAME VARCHAR(100),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ID int,
  MSISDN varchar(15),
  DATE_MKT varchar(32),
  HEURE_MKT varchar(32),
  CREATED DATE,
  MODIFIED DATE,
  MB_CHALLENGE_USERS_ID int
)COMMENT 'CDR SPARK_TT_CHALLENGE_USER_PULSE external table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
LOCATION '/PROD/TT/PERCO_Q1_2021/CHALLENGE_USER_PULSE'
TBLPROPERTIES ('serialization.null.format'='');
