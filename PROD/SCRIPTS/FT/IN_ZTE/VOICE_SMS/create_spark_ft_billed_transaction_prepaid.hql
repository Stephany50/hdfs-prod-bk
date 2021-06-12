
CREATE TABLE JUNK.SPARK_FT_BILLED_TRANSACTION_PREPAID (
    SERVED_PARTY  VARCHAR(40),
    OTHER_PARTY  VARCHAR(40),
    RATED_DURATION  INT,
    CALL_PROCESS_TOTAL_DURATION  INT,
    RATED_VOLUME  INT,
    UNIT_OF_MEASUREMENT  VARCHAR(50),
    SERVICE_CODE  VARCHAR(50),
    TELESERVICE_INDICATOR  VARCHAR(50),
    NETWORK_EVENT_TYPE  VARCHAR(50),
    NETWORK_ELEMENT_ID  VARCHAR(50),
    OTHER_PARTY_ZONE  VARCHAR(50),
    CALL_DESTINATION_CODE  VARCHAR(50),
    BILLING_TERM_INDICATOR  VARCHAR(50),
    NETWORK_TERM_INDICATOR  VARCHAR(50),
    SERVED_PARTY_IMSI  VARCHAR(50),
    RAW_SPECIFIC_CHARGINGINDICATOR  VARCHAR(50),
    RAW_LOAD_LEVEL_INDICATOR  VARCHAR(20),
    FEE_NAME  VARCHAR(50),
    REFILL_TOPUP_PROFILE  VARCHAR(50),
    MAIN_REFILL_AMOUNT  DOUBLE,
    BUNDLE_REFILL_AMOUNT  BIGINT,
    RAW_TRANSNUM_USED_FOR_REFILL  VARCHAR(50),
    COMMERCIAL_OFFER  VARCHAR(50),
    COMMERCIAL_PROFILE  VARCHAR(50),
    LOCATION_MCC  VARCHAR(50),
    LOCATION_MNC  VARCHAR(50),
    LOCATION_LAC  VARCHAR(50),
    LOCATION_CI  VARCHAR(50),
    MAIN_RATED_AMOUNT  DOUBLE,
    PROMO_RATED_AMOUNT  DOUBLE,
    BUNDLE_IDENTIFIER  VARCHAR(50),
    BUNDLE_UNIT  VARCHAR(50),
    BUNDLE_CONSUMED_VOLUME  BIGINT,
    BUNDLE_DISCARDED_VOLUME  BIGINT,
    BUNDLE_REMAINING_VOLUME  BIGINT,
    BUNDLE_REFILL_VOLUME  BIGINT,
    RATED_AMOUNT_IN_BUNDLE  DOUBLE,
    MAIN_REMAINING_CREDIT  DOUBLE,
    PROMO_REMAINING_CREDIT  DOUBLE,
    SPECIFIC_TARIFF_INDICATOR  VARCHAR(150),
    LOCATION_NUMBER  VARCHAR(40),
    MAIN_DISCARDED_CREDIT  DOUBLE,
    PROMO_DISCARDED_CREDIT  DOUBLE,
    SMS_DISCARDED_VOLUME  INT,
    SMS_USED_VOLUME  INT,
    RAW_TARIFF_PLAN  VARCHAR(100),
    RAW_EVENT_COST  DOUBLE,
    RAW_REFILL_MEANS  VARCHAR(50),
    RAW_CALL_TYPE  VARCHAR(50),
    CALL_DESTINATION_TYPE  VARCHAR(30),
    ROAMING_INDICATOR  INT,
    OPERATOR_CODE  VARCHAR(30),
    YZDISCOUNT  INT,
    LOCATION_LAC_DECIMAL  VARCHAR(15),
    LOCATION_CI_DECIMAL  VARCHAR(15),
    CHARGE_SUM  BIGINT,
    BYZ_RATED_AMOUNT  DOUBLE,
    BUNDLE_SMS_USED_VOLUME  BIGINT,
    BUNDLE_TIME_USED_VOLUME  BIGINT,
    UNKNOWN_USED_VOLUME  BIGINT, 
    BUNDLE_SMS_REMAINING_VOLUME  BIGINT,
    BUNDLE_TIME_REMAINING_VOLUME  BIGINT,
    UPLOAD_VOLUME  BIGINT,
    DOWNLOAD_VOLUME  BIGINT,
    IDENTIFIER_LIST  VARCHAR(4000),
    UNIT_OF_MEASUREMENT_LIST  VARCHAR(4000),
    RATED_VOLUME_LIST  VARCHAR(4000),
    REMAINING_VOLUME_LIST  VARCHAR(4000),
    TRANSACTION_TYPE  VARCHAR(30),
    SERVED_PARTY_IMEI  VARCHAR(50),
    CHARGED_PARTY  VARCHAR(50),
    TRANSACTION_TERM_INDICATOR  INT,
    UNKNOWN_VOLUME_LIST  VARCHAR(4000),
    VOLUME_LIST  VARCHAR(4000),
    ORIGINAL_COMMERCIAL_PROFILE  VARCHAR(50),
    TRANSACTION_TIME VARCHAR(10),
    ORIGINAL_FILE_NAME  VARCHAR(60),
    SOURCE_PLATEFORM  VARCHAR(30),
    SOURCE_DATA  VARCHAR(30),
    INSERT_DATE  TIMESTAMP,
    vlr_number varchar(100)
)
COMMENT 'Voice SMS Fact Table'
PARTITIONED BY (TRANSACTION_DATE  DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');



INSERT INTO tmp.FT_BILLED_TRANSACTION_PREPAID SELECT
    SERVED_PARTY,
    OTHER_PARTY,
    RATED_DURATION,
    CALL_PROCESS_TOTAL_DURATION,
    RATED_VOLUME,
    UNIT_OF_MEASUREMENT,
    SERVICE_CODE,
    TELESERVICE_INDICATOR,
    NETWORK_EVENT_TYPE,
    NETWORK_ELEMENT_ID,
    OTHER_PARTY_ZONE,
    CALL_DESTINATION_CODE,
    BILLING_TERM_INDICATOR,
    NETWORK_TERM_INDICATOR,
    SERVED_PARTY_IMSI,
    RAW_SPECIFIC_CHARGINGINDICATOR,
    RAW_LOAD_LEVEL_INDICATOR,
    FEE_NAME,
    REFILL_TOPUP_PROFILE,
    MAIN_REFILL_AMOUNT,
    BUNDLE_REFILL_AMOUNT,
    RAW_TRANSNUM_USED_FOR_REFILL,
    COMMERCIAL_OFFER,
    COMMERCIAL_PROFILE,
    LOCATION_MCC,
    LOCATION_MNC,
    LOCATION_LAC,
    LOCATION_CI,
    MAIN_RATED_AMOUNT,
    PROMO_RATED_AMOUNT,
    BUNDLE_IDENTIFIER,
    BUNDLE_UNIT,
    BUNDLE_CONSUMED_VOLUME,
    BUNDLE_DISCARDED_VOLUME,
    BUNDLE_REMAINING_VOLUME,
    BUNDLE_REFILL_VOLUME,
    RATED_AMOUNT_IN_BUNDLE,
    MAIN_REMAINING_CREDIT,
    PROMO_REMAINING_CREDIT,
    SPECIFIC_TARIFF_INDICATOR,
    LOCATION_NUMBER,
    MAIN_DISCARDED_CREDIT,
    PROMO_DISCARDED_CREDIT,
    SMS_DISCARDED_VOLUME,
    SMS_USED_VOLUME,
    RAW_TARIFF_PLAN,
    RAW_EVENT_COST,
    RAW_REFILL_MEANS,
    RAW_CALL_TYPE,
    CALL_DESTINATION_TYPE,
    ROAMING_INDICATOR,
    OPERATOR_CODE,
    YZDISCOUNT,
    LOCATION_LAC_DECIMAL,
    LOCATION_CI_DECIMAL,
    CHARGE_SUM,
    BYZ_RATED_AMOUNT,
    BUNDLE_SMS_USED_VOLUME,
    BUNDLE_TIME_USED_VOLUME,
    UNKNOWN_USED_VOLUME,
    BUNDLE_SMS_REMAINING_VOLUME,
    BUNDLE_TIME_REMAINING_VOLUME,
    UPLOAD_VOLUME,
    DOWNLOAD_VOLUME,
    IDENTIFIER_LIST,
    UNIT_OF_MEASUREMENT_LIST,
    RATED_VOLUME_LIST,
    REMAINING_VOLUME_LIST,
    TRANSACTION_TYPE,
    SERVED_PARTY_IMEI,
    CHARGED_PARTY,
    TRANSACTION_TERM_INDICATOR,
    UNKNOWN_VOLUME_LIST,
    VOLUME_LIST,
    ORIGINAL_COMMERCIAL_PROFILE,
    substring(TRANSACTION_DATE,12,8) TRANSACTION_DATE,
    ORIGINAL_FILE_NAME,
    SOURCE_PLATEFORM,
    SOURCE_DATA,
    INSERT_DATE,
    to_date(TRANSACTION_DATE)
from backup_dwh.FT_BILLED_TRANSACTION_PREPAID