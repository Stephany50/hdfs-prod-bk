CREATE TABLE AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT
(
    DESTINATION_CODE        VARCHAR(50),
    PROFILE_CODE            VARCHAR(50),
    SUB_ACCOUNT             VARCHAR(50),
    MEASUREMENT_UNIT        VARCHAR(50),
    SOURCE_TABLE            VARCHAR(50),
    OPERATOR_CODE           VARCHAR(50),
    TOTAL_AMOUNT            DOUBLE,
    RATED_AMOUNT            DOUBLE,
    INSERT_DATE             TIMESTAMP,
    REGION_ID               VARCHAR(50)
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')