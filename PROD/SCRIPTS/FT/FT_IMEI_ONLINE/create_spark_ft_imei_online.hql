CREATE TABLE MON.SPARK_FT_IMEI_ONLINE (
    IMEI VARCHAR(50),
    IMSI VARCHAR(50),
    MSISDN VARCHAR(50),
    SRC_TABLE VARCHAR(100),
    TRANSACTION_COUNT INT,
    INSERT_DATE TIMESTAMP
) COMMENT 'FT_IMEI_ONLINE - FT'
  PARTITIONED BY (SDATE    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
