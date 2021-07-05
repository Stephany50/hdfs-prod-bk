CREATE TABLE MON.SPARK_FT_OTARIE_USERS_TRAFFIC (
    MSISDN VARCHAR(40),
    traffic_daily_up BIGINT,
    traffic_7_days_up BIGINT,
    traffic_mtd_up BIGINT,
    traffic_30_days_up BIGINT,
    traffic_daily_down BIGINT,
    traffic_7_days_down BIGINT,
    traffic_mtd_down BIGINT,
    traffic_30_days_down BIGINT,
    traffic_daily_test BIGINT,
    traffic_7_days_test BIGINT,
    traffic_mtd_test BIGINT,
    traffic_30_days_test BIGINT,
    INSERT_DATE TIMESTAMP
)
    PARTITIONED BY (TRANSACTION_DATE DATE)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY")