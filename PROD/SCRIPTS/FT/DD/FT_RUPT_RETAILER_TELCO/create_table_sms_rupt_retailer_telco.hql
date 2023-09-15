CREATE TABLE IF NOT EXISTS DD.SMS_RUPT_RETAILER_TELCO(
    MSISDN STRING,
    SMS STRING,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE, EVENT_TIME TIMESTAMP)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');