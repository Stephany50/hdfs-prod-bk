CREATE TABLE CDR.SPARK_IT_SMSC_MVAS_A2P_CHECKFILES (
    FILE_NAME VARCHAR(50),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_NAME VARCHAR(100),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(FILE_NAME) INTO 2 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')