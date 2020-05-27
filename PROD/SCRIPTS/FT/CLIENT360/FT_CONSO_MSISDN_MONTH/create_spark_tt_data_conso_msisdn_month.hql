CREATE TABLE MON.SPARK_TT_DATA_CONSO_MSISDN_MONTH
(
    MSISDN VARCHAR(25),
    COMMERCIAL_OFFER VARCHAR(250),
    BYTES_SENT DECIMAL(17,2),
    BYTES_RECEIVED DECIMAL(17,2),
    MMS_COUNT DECIMAL(17,2),
    MAIN_RATED_AMOUNT DECIMAL(17,2),
    PROMO_RATED_AMOUNT DECIMAL(17,2),
    SERVED_PARTY_IMSI VARCHAR(50),
    SERVED_PARTY_IMEI VARCHAR(50),
    BYTES_USED_IN_BUNDLE DECIMAL(17,2),
    BYTES_USED_OUT_BUNDLE DECIMAL(17,2),
    BYTES_USED_IN_BUNDLE_ROAMING DECIMAL(17,2),
    BYTES_USED_OUT_BUNDLE_ROAMING DECIMAL(17,2),
    BUNDLE_MMS_USED_VOLUME DECIMAL(17,2),
    MAIN_RATED_AMOUNT_ROAMING DECIMAL(17,2),
    PROMO_RATED_AMOUNT_ROAMING DECIMAL(17,2),
    GOS_DEBIT_COUNT DECIMAL(17,2),
    GOS_SESSION_COUNT DECIMAL(17,2),
    GOS_REFUND_COUNT DECIMAL(17,2),
    GOS_DEBIT_AMOUNT DECIMAL(17,2),
    GOS_SESSION_AMOUNT DECIMAL(17,2),
    GOS_REFUND_AMOUNT DECIMAL(17,2),
    BUNDLE_BYTES_REMAINING_VOLUME DECIMAL(17,2),
    BUNDLE_MMS_REMAINING_VOLUME DECIMAL(17,2),
    SOURCE_TABLE VARCHAR(250),
    OPERATOR_CODE VARCHAR(250),
    ACTIVE_DAYS_COUNT DECIMAL(17,2),
    FIRST_ACTIVE_DAY DATE,
    LAST_ACTIVE_DAY DATE,
    MAX_BYTES_USED DECIMAL(17,2),
    MONTH_MAX_BYTES_USED VARCHAR(25),
    INSERT_DATE timestamp
)
    COMMENT 'SPARK_TT_DATA_CONSO_MSISDN_MONTH'
    PARTITIONED BY (EVENT_MONTH VARCHAR(25))
    STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY");