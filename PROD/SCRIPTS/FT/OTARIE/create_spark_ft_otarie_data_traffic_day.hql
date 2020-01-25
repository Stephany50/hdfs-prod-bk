CREATE TABLE MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY (
    MSISDN VARCHAR(40),
    APPLI_TYPE VARCHAR(100),
    IDAPN VARCHAR(50),
    RADIO_ACCESS_TECHNO VARCHAR(100),
    ROAMING VARCHAR(20),
    NBYTESDN BIGINT,
    NBYTESUP BIGINT,
    NBYTEST BIGINT,
    TERMINAL_TYPE VARCHAR(300),
    TERMINAL_BRAND VARCHAR(300),
    TERMINAL_MODEL VARCHAR(300),
    COMMERCIAL_OFFER VARCHAR(100),
    IMEI VARCHAR(50),
    INSERT_DATE TIMESTAMP,
    ORIGINAL_FILE_NAME VARCHAR(300),
    ORIGINAL_FILE_DATE DATE
)COMMENT 'SPARK FT OTARIE DATA TRAFFIC DAILY'
    PARTITIONED BY (TRANSACTION_DATE DATE)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY")