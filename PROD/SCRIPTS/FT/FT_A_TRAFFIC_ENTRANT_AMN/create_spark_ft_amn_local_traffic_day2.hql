CREATE TABLE MON.SPARK_FT_AMN_LOCAL_TRAFFIC_DAY2
(
    SITE_NAME    VARCHAR(100),
    DURATION     BIGINT,
    SMS_COUNT    BIGINT,
    INSERT_DATE  DATE
) COMMENT 'SPARK_FT_AMN_LOCAL_TRAFFIC_DAY2 table'
    PARTITIONED BY (EVENT_DATE   DATE)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY")