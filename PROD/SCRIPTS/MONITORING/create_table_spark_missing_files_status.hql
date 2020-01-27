
CREATE TABLE IF NOT EXISTS MON.SPARK_MISSING_FILES_STATUS(
    JOB_SOURCE STRING,
     STATUS STRING,
    INSERT_DATE TIMESTAMP
)

PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


