CREATE  TABLE CDR.SPARK_IT_DEPOCREDI
(

    Customer_id VARCHAR(200),
    Subs_id VARCHAR(200),
    ACCNBR VARCHAR(200),
    Adjust_balance_amount VARCHAR(200),
    Adjust_balance_date DATE,
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE VARCHAR(200),
    ORIGINAL_FILE_LINE_COUNT VARCHAR(200),
    INSERT_DATE timestamp
)
    PARTITIONED BY (original_file_date DATE)
    CLUSTERED BY(Customer_id) INTO 8 BUCKETS
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY");