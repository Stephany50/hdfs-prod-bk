
CREATE TABLE MON.FT_VAS_REVENUE_DETAIL
(
    NQ_TRANSACTION_DATE TIMESTAMP,
    SERVED_PARTY VARCHAR(40),
    OTHER_PARTY VARCHAR(40),
    RATED_DURATION DECIMAL(12,2),
    CALL_PROCESS_TOTAL_DURATION DECIMAL(12,2),
    RATED_VOLUME DECIMAL(12,2),
    UNIT_OF_MEASUREMENT VARCHAR(50),
    SERVICE_CODE VARCHAR(50),
    TELESERVICE_INDICATOR VARCHAR(50),
    NETWORK_EVENT_TYPE VARCHAR(50),
    NETWORK_ELEMENT_ID VARCHAR(50),
    OTHER_PARTY_ZONE VARCHAR(50),
    CALL_DESTINATION_CODE VARCHAR(50),
    BILLING_TERM_INDICATOR VARCHAR(50),
    NETWORK_TERM_INDICATOR VARCHAR(50),
    SERVED_PARTY_IMSI VARCHAR(50),
    RAW_SPECIFIC_CHARGINGINDICATOR VARCHAR(150),
    RAW_LOAD_LEVEL_INDICATOR VARCHAR(20),
    FEE_NAME VARCHAR(50),
    REFILL_TOPUP_PROFILE VARCHAR(50),
    MAIN_REFILL_AMOUNT DECIMAL(12,2),
    BUNDLE_REFILL_AMOUNT DECIMAL(12,2),
    RAW_TRANSNUM_USED_FOR_REFILL VARCHAR(50),
    COMMERCIAL_OFFER VARCHAR(50),
    COMMERCIAL_PROFILE VARCHAR(50),
    LOCATION_MCC VARCHAR(50),
    LOCATION_MNC VARCHAR(50),
    LOCATION_LAC VARCHAR(50),
    LOCATION_CI VARCHAR(50),
    MAIN_RATED_AMOUNT DECIMAL(12,2),
    PROMO_RATED_AMOUNT DECIMAL(12,2),
    BUNDLE_IDENTIFIER VARCHAR(50),
    BUNDLE_UNIT VARCHAR(50),
    BUNDLE_CONSUMED_VOLUME DECIMAL(12,2),
    BUNDLE_DISCARDED_VOLUME DECIMAL(12,2),
    BUNDLE_REMAINING_VOLUME DECIMAL(12,2),
    BUNDLE_REFILL_VOLUME DECIMAL(12,2),
    RATED_AMOUNT_IN_BUNDLE DECIMAL(12,2),
    MAIN_REMAINING_CREDIT DECIMAL(12,2),
    PROMO_REMAINING_CREDIT DECIMAL(12,2),
    SPECIFIC_TARIFF_INDICATOR VARCHAR(150),
    LOCATION_NUMBER VARCHAR(40),
    MAIN_DISCARDED_CREDIT DECIMAL(12,2),
    PROMO_DISCARDED_CREDIT DECIMAL(12,2),
    SMS_DISCARDED_VOLUME DECIMAL(12,2),
    SMS_USED_VOLUME DECIMAL(12,2),
    RAW_TARIFF_PLAN VARCHAR(50),
    RAW_EVENT_COST DECIMAL(12,2),
    RAW_REFILL_MEANS VARCHAR(50),
    RAW_CALL_TYPE VARCHAR(50),
    ORIGINAL_FILE_NAME VARCHAR(90),
    SOURCE_PLATEFORM VARCHAR(30),
    SOURCE_DATA VARCHAR(30),
    INSERT_DATE DATE
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS ORC TBLPROPERTIES ("orc.compress"="ZLIB","orc.stripe.size"="67108864")
