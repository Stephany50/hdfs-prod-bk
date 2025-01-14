CREATE TABLE MON.SPARK_FT_RATING_GROUP
(
  RATINGGROUP         VARCHAR(100),
  SERVICE             VARCHAR(100),
  BYTES_SENT         BIGINT,
  BYTES_RECEIVED     BIGINT,
  BYTES_USED        BIGINT,
  INSERT_DATE         TIMESTAMP
)
PARTITIONED BY (EVENT_DATE    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')