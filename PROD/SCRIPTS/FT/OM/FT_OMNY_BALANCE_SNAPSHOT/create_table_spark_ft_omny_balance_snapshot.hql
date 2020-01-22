CREATE TABLE MON.SPARK_FT_OMNY_BALANCE_SNAPSHOT
(
  USER_CATEGORY       VARCHAR(50),
  USER_ID             VARCHAR(50),
  MSISDN              VARCHAR(50),
  BALANCE             DECIMAL(17,2),
  INSERT_DATE         TIMESTAMP,
  PARTNER_FIRST_NAME  VARCHAR(90),
  PARTNER_LAST_NAME   VARCHAR(90),
  ACCOUNT_NUMBER      VARCHAR(90)
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')