
CREATE TABLE MON.FT_CRA_GPRS (
    TRANSACTION_ID	VARCHAR(100),
    CALL_TYPE	VARCHAR(50),
    SERVED_PARTY_MSISDN	VARCHAR(100),
    OTHER_PARTY_MSISDN	VARCHAR(100),
    SERVED_PARTY_OFFER	VARCHAR(100),
    SERVED_PARTY_TYPE	VARCHAR(100),
    SESSION_TIME	VARCHAR(8),
    BYTES_SENT	BIGINT,
    BYTES_RECEIVED	BIGINT,
    TOTAL_COST	DOUBLE,
    SESSION_DURATION	INT,
    CONTENT_PROVIDER	VARCHAR(100),
    SERVICE_TYPE	VARCHAR(100),
    SERVICE_CODE	VARCHAR(100),
    TRANSACTION_TERMINATION_IND	INT,
    SERVED_PARTY_IMSI	VARCHAR(100),
    SERVED_PARTY_IMEI	VARCHAR(100),
    TOTAL_HITS	DOUBLE,
    TOTAL_UNIT	DOUBLE,
    TOTAL_COMMODITIES	INT,
    TOTAL_ERRORS	DOUBLE,
    SGSN	VARCHAR(100),
    GGSN	VARCHAR(100),
    APN	VARCHAR(100),
    FIRST_URL	VARCHAR(4000),
    ERROR_LIST	VARCHAR(2000),
    COUNTRY_DESTINATION_LIST	VARCHAR(1000),
    UNIT_OF_MEASUREMENT	VARCHAR(50),
    UNITS_USED_IN_BUNDLE	INT,
    ERROR_LIST_IN	VARCHAR(1000),
    DOWNLOAD_STATUS	VARCHAR(50),
    OPERATOR_CODE	VARCHAR(25),
    SERVICE_CATEGORY	VARCHAR(50),
    SERVED_PARTY_PRICE_PLAN	VARCHAR(100),
    CHARGED_PARTY_MSISDN	VARCHAR(25),
    ROAMING_INDICATOR	VARCHAR(5),
    LOCATION_MCC	VARCHAR(25),
    LOCATION_MNC	VARCHAR(25),
    LOCATION_LAC	VARCHAR(25),
    LOCATION_CI	VARCHAR(25),
    PDP_ADDRESS	VARCHAR(25),
    MAIN_COST	DOUBLE,
    PROMO_COST	DOUBLE,
    BUNDLE_BYTES_USED_VOLUME	BIGINT,
    BUNDLE_MMS_USED_VOLUME	BIGINT,
    CHARGE_SUM	BIGINT,
    USED_VOLUME_LIST	VARCHAR(255),
    USED_BALANCE_LIST	VARCHAR(255),
    USED_UNIT_LIST	VARCHAR(255),
    MAIN_REMAINING_CREDIT	DOUBLE,
    PROMO_REMAINING_CREDIT	DOUBLE,
    BUNDLE_BYTES_REMAINING_VOLUME	BIGINT,
    BUNDLE_MMS_REMAINING_VOLUME	BIGINT,
    REMAINING_VOLUME_LIST	VARCHAR(255),
    TOTAL_OCCURENCE	INT,
    SOURCE_PLATFORM	VARCHAR(25),
    SOURCE_DATA	VARCHAR(40),
    PRECEDING_VOLUME_LIST	VARCHAR(150),
    SDP_GOS_SERV_NAME	VARCHAR(50),
    SDP_GOS_SERV_DETAIL	VARCHAR(75),
    DWH_IT_ENTRY_DATE	TIMESTAMP,
    DWH_FT_ENTRY_DATE	TIMESTAMP,
    ORIGINAL_FILE_NAME	VARCHAR(100) ) COMMENT 'MON.FT_CRA_GPRS Table'
PARTITIONED BY (SESSION_DATE	DATE)
STORED AS ORC TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;

