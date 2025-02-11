CREATE  TABLE CDR.SPARK_IT_RATEPLAN
(
    Default_price_plan_ID VARCHAR(200),
    DESCRIPTION VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE VARCHAR(200),
    ORIGINAL_FILE_LINE_COUNT VARCHAR(200),
    INSERT_DATE timestamp
)
    PARTITIONED BY (original_file_date DATE)
    CLUSTERED BY(Default_price_plan_ID) INTO 8 BUCKETS
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY");