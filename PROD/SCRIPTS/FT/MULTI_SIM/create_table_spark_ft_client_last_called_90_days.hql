CREATE TABLE MON.SPARK_FT_CLIENT_LAST_CALLED_90_DAYS (
    CALLER VARCHAR(100),
    CALLEE VARCHAR(100),
    LAST_CALL_DATE DATE,
    INSERT_DATE TIMESTAMP
)
COMMENT 'Table containing distinct callees over a 90 days window for a caller'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
