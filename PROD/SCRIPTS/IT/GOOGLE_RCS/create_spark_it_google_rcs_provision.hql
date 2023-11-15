CREATE TABLE CDR.SPARK_IT_GOOGLE_RCS_PROVISION (
    MSISDN VARCHAR(200),
    TENANT VARCHAR(200),
    INSERT_DATE DATE,
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT
)
PARTITIONED BY (START_DATE DATE,FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
