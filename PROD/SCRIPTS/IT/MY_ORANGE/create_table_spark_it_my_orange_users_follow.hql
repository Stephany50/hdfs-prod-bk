CREATE TABLE CDR.SPARK_IT_MY_ORANGE_USERS_FOLLOW (
    NBRE_USERS_FOLLOW BIGINT,
    --ORIGINAL_FILE_SIZE INT,
    --ORIGINAL_FILE_LINE_COUNT INT,
    --ORIGINAL_FILE_NAME VARCHAR(200),
    --ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP,
    EVENT_DATE DATE
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')