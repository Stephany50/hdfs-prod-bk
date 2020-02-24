CREATE TABLE MON.SPARK_FT_SUBSCRIBER_LOCATION 
(
    MSISDN VARCHAR(50), 
    SERVED_LOCATION VARCHAR(200), 
    RAW_LAC VARCHAR(50), 
    RAW_CI VARCHAR(50), 
    SMS_COUNT INT, 
    CALL_COUNT INT,
    CALL_DURATION DOUBLE, 
    MAIN_RATED_AMOUNT DOUBLE, 
    PROMO_RATED_AMOUNT DOUBLE, 
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')