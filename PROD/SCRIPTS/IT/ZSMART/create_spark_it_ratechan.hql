CREATE  TABLE CDR.SPARK_IT_RATECHAN (
    Subs_id VARCHAR(200)
    ,OLD_DEFAULT_PRICE_PLAN_ID VARCHAR(200)
    ,NEW_DEFAULT_PRICE_PLAN_ID VARCHAR(200)
    ,Update_date timestamp
    ,CUID VARCHAR(200)
    ,ORIGINAL_FILE_NAME VARCHAR(200)
    ,ORIGINAL_FILE_SIZE INT
    ,ORIGINAL_FILE_LINE_COUNT VARCHAR(200)
    ,INSERT_DATE timestamp
)
    PARTITIONED BY (original_file_date DATE)
    CLUSTERED BY(Subs_id) INTO 8 BUCKETS
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY");