
CREATE TABLE MON.SPARK_FT_CRA_GPRS (
    TRANSACTION_ID  VARCHAR(100),
    CALL_TYPE  VARCHAR(50),
    SERVED_PARTY_MSISDN  VARCHAR(100),
    OTHER_PARTY_MSISDN  VARCHAR(100),
    SERVED_PARTY_OFFER  VARCHAR(100),
    SERVED_PARTY_TYPE  VARCHAR(100),
    SESSION_TIME  VARCHAR(8),
    BYTES_SENT  BIGINT,
    BYTES_RECEIVED  BIGINT,
    TOTAL_COST  DOUBLE,
    SESSION_DURATION  INT,
    CONTENT_PROVIDER  VARCHAR(100),
    SERVICE_TYPE  VARCHAR(100),
    SERVICE_CODE  VARCHAR(100),
    TRANSACTION_TERMINATION_IND  INT,
    SERVED_PARTY_IMSI  VARCHAR(100),
    SERVED_PARTY_IMEI  VARCHAR(100),
    TOTAL_HITS  DOUBLE,
    TOTAL_UNIT  DOUBLE,
    TOTAL_COMMODITIES  INT,
    TOTAL_ERRORS  DOUBLE,
    SGSN  VARCHAR(100),
    GGSN  VARCHAR(100),
    APN  VARCHAR(100),
    FIRST_URL  VARCHAR(4000),
    ERROR_LIST  VARCHAR(2000),
    COUNTRY_DESTINATION_LIST  VARCHAR(1000),
    UNIT_OF_MEASUREMENT  VARCHAR(50),
    UNITS_USED_IN_BUNDLE  INT,
    ERROR_LIST_IN  VARCHAR(1000),
    DOWNLOAD_STATUS  VARCHAR(50),
    OPERATOR_CODE  VARCHAR(25),
    SERVICE_CATEGORY  VARCHAR(50),
    SERVED_PARTY_PRICE_PLAN  VARCHAR(100),
    CHARGED_PARTY_MSISDN  VARCHAR(25),
    ROAMING_INDICATOR  VARCHAR(5),
    LOCATION_MCC  VARCHAR(25),
    LOCATION_MNC  VARCHAR(25),
    LOCATION_LAC  VARCHAR(25),
    LOCATION_CI  VARCHAR(25),
    PDP_ADDRESS  VARCHAR(25),
    MAIN_COST  DOUBLE,
    PROMO_COST  DOUBLE,
    BUNDLE_BYTES_USED_VOLUME  BIGINT,
    BUNDLE_MMS_USED_VOLUME  BIGINT,
    CHARGE_SUM  BIGINT,
    USED_VOLUME_LIST  VARCHAR(255),
    USED_BALANCE_LIST  VARCHAR(255),
    USED_UNIT_LIST  VARCHAR(255),
    MAIN_REMAINING_CREDIT  DOUBLE,
    PROMO_REMAINING_CREDIT  DOUBLE,
    BUNDLE_BYTES_REMAINING_VOLUME  BIGINT,
    BUNDLE_MMS_REMAINING_VOLUME  BIGINT,
    REMAINING_VOLUME_LIST  VARCHAR(255),
    TOTAL_OCCURENCE  INT,
    SOURCE_PLATFORM  VARCHAR(25),
    SOURCE_DATA  VARCHAR(40),
    PRECEDING_VOLUME_LIST  VARCHAR(150),
    SDP_GOS_SERV_NAME  VARCHAR(50),
    SDP_GOS_SERV_DETAIL  VARCHAR(75),
    DWH_IT_ENTRY_DATE  TIMESTAMP,
    DWH_FT_ENTRY_DATE  TIMESTAMP,
    ORIGINAL_FILE_NAME  VARCHAR(100),
    GPP_USER_LOCATION_INFO  VARCHAR(300)
) COMMENT 'MON.FT_CRA_GPRS Table'
PARTITIONED BY (SESSION_DATE  DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;


create table backup_dwh.ft_cra_gprs_20190923_final (
    TRANSACTION_ID  VARCHAR(255),
    CALL_TYPE  VARCHAR(255),
    SERVED_PARTY_MSISDN  VARCHAR(255),
    OTHER_PARTY_MSISDN  VARCHAR(255),
    SERVED_PARTY_OFFER  VARCHAR(255),
    SERVED_PARTY_TYPE  VARCHAR(255),
    SESSION_DATE  VARCHAR(255),
    SESSION_TIME  VARCHAR(255),
    BYTES_SENT  VARCHAR(255),
    BYTES_RECEIVED  VARCHAR(255),
    TOTAL_COST  VARCHAR(255),
    SESSION_DURATION  VARCHAR(255),
    CONTENT_PROVIDER  VARCHAR(255),
    SERVICE_TYPE  VARCHAR(255),
    SERVICE_CODE  VARCHAR(255),
    TRANSACTION_TERMINATION_IND  VARCHAR(255),
    SERVED_PARTY_IMSI  VARCHAR(255),
    SERVED_PARTY_IMEI  VARCHAR(255),
    TOTAL_HITS  VARCHAR(255),
    TOTAL_UNIT  VARCHAR(255),
    TOTAL_COMMODITIES  VARCHAR(255),
    TOTAL_ERRORS  VARCHAR(255),
    SGSN  VARCHAR(255),
    GGSN  VARCHAR(255),
    APN  VARCHAR(255),
    FIRST_URL  VARCHAR(255),
    ERROR_LIST  VARCHAR(255),
    COUNTRY_DESTINATION_LIST  VARCHAR(255),
    UNIT_OF_MEASUREMENT  VARCHAR(255),
    UNITS_USED_IN_BUNDLE  VARCHAR(255),
    ERROR_LIST_IN  VARCHAR(255),
    DOWNLOAD_STATUS  VARCHAR(255),
    DWH_IT_ENTRY_DATE  VARCHAR(255),
    DWH_FT_ENTRY_DATE  VARCHAR(255),
    ORIGINAL_FILE_NAME  VARCHAR(255),
    OPERATOR_CODE  VARCHAR(255),
    SERVICE_CATEGORY  VARCHAR(255),
    SERVED_PARTY_PRICE_PLAN  VARCHAR(255),
    CHARGED_PARTY_MSISDN  VARCHAR(255),
    ROAMING_INDICATOR  VARCHAR(255),
    LOCATION_MCC  VARCHAR(255),
    LOCATION_MNC  VARCHAR(255),
    LOCATION_LAC  VARCHAR(255),
    LOCATION_CI  VARCHAR(255),
    PDP_ADDRESS  VARCHAR(255),
    MAIN_COST  VARCHAR(255),
    PROMO_COST  VARCHAR(255),
    BUNDLE_BYTES_USED_VOLUME  VARCHAR(255),
    BUNDLE_MMS_USED_VOLUME  VARCHAR(255),
    CHARGE_SUM  VARCHAR(255),
    USED_VOLUME_LIST  VARCHAR(255),
    USED_BALANCE_LIST  VARCHAR(255),
    USED_UNIT_LIST  VARCHAR(255),
    MAIN_REMAINING_CREDIT  VARCHAR(255),
    PROMO_REMAINING_CREDIT  VARCHAR(255),
    BUNDLE_BYTES_REMAINING_VOLUME  VARCHAR(255),
    BUNDLE_MMS_REMAINING_VOLUME  VARCHAR(255),
    REMAINING_VOLUME_LIST  VARCHAR(255),
    TOTAL_OCCURENCE  VARCHAR(255),
    SOURCE_PLATFORM  VARCHAR(255),
    SOURCE_DATA  VARCHAR(255),
    PRECEDING_VOLUME_LIST  VARCHAR(255),
    SDP_GOS_SERV_NAME  VARCHAR(255),
    SDP_GOS_SERV_DETAIL  VARCHAR(255),
    GPP_USER_LOCATION_INFO  VARCHAR(255)
)
stored as orc


INSERT INTO TMP.FT_CRA_GPRS
SELECT
    TRANSACTION_ID,
    CALL_TYPE,
    SERVED_PARTY_MSISDN,
    OTHER_PARTY_MSISDN,
    SERVED_PARTY_OFFER,
    SERVED_PARTY_TYPE,
    SESSION_TIME,
    BYTES_SENT,
    BYTES_RECEIVED,
    TOTAL_COST,
    SESSION_DURATION,
    CONTENT_PROVIDER,
    SERVICE_TYPE,
    SERVICE_CODE,
    TRANSACTION_TERMINATION_IND,
    SERVED_PARTY_IMSI,
    SERVED_PARTY_IMEI,
    TOTAL_HITS,
    TOTAL_UNIT,
    TOTAL_COMMODITIES,
    TOTAL_ERRORS,
    SGSN,
    GGSN,
    APN,
    FIRST_URL,
    ERROR_LIST,
    COUNTRY_DESTINATION_LIST,
    UNIT_OF_MEASUREMENT,
    UNITS_USED_IN_BUNDLE,
    ERROR_LIST_IN,
    DOWNLOAD_STATUS,
    OPERATOR_CODE,
    SERVICE_CATEGORY,
    SERVED_PARTY_PRICE_PLAN,
    CHARGED_PARTY_MSISDN,
    ROAMING_INDICATOR,
    LOCATION_MCC,
    LOCATION_MNC,
    LOCATION_LAC,
    LOCATION_CI,
    PDP_ADDRESS,
    MAIN_COST,
    PROMO_COST,
    BUNDLE_BYTES_USED_VOLUME,
    BUNDLE_MMS_USED_VOLUME,
    CHARGE_SUM,
    USED_VOLUME_LIST,
    USED_BALANCE_LIST,
    USED_UNIT_LIST,
    MAIN_REMAINING_CREDIT,
    PROMO_REMAINING_CREDIT,
    BUNDLE_BYTES_REMAINING_VOLUME,
    BUNDLE_MMS_REMAINING_VOLUME,
    REMAINING_VOLUME_LIST,
    TOTAL_OCCURENCE,
    SOURCE_PLATFORM,
    SOURCE_DATA,
    PRECEDING_VOLUME_LIST,
    SDP_GOS_SERV_NAME,
    SDP_GOS_SERV_DETAIL,
    DWH_IT_ENTRY_DATE,
    DWH_FT_ENTRY_DATE,
    ORIGINAL_FILE_NAME,
    to_date(SESSION_DATE)
from backup_dwh.ft_cra_gprs_20190923_final