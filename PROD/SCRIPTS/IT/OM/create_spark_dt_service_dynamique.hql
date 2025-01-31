CREATE TABLE CDR.DT_SERVICES_DYNAMIQUE
(
  MSISDN     VARCHAR(400),
  BDLE_NAME  VARCHAR(400 ),
  BDLE_COST  DOUBLE,
  ORIGINAL_FILE_NAME VARCHAR(50),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(MSISDN) INTO 32 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
