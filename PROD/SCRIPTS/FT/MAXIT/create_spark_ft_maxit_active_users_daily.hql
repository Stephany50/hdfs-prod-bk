
CREATE TABLE MON.SPARK_FT_MAXIT_ACTIVE_USERS_DAILY (
    MSISDN VARCHAR(200),
    CANAL VARCHAR(200),
    INSERT_DATE TIMESTAMP,
    ORIGINAL_FILE_NAME VARCHAR(200)
)COMMENT 'MON_SPARK_FT_MAXIT_ACTIVE_USERS_DAILY'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')