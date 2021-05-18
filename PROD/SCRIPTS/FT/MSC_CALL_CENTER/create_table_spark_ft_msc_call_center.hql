CREATE TABLE MON.SPARK_FT_MSC_CALL_CENTER
(
    TRANSACTION_TIME        VARCHAR(6),
    SERVED_MSISDN            VARCHAR(40),
    OTHER_PARTY             VARCHAR(40),
    TRANSACTION_TYPE        VARCHAR(40),
    TRANSACTION_DIRECTION   VARCHAR(40),
    INSERT_DATE             TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE  DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')