CREATE TABLE MON.SPARK_FT_SUBSCRIPTION (
    TRANSACTION_TIME  VARCHAR (6),
    SERVED_PARTY_MSISDN  VARCHAR (40),
    CONTRACT_TYPE   VARCHAR (50),
    COMMERCIAL_OFFER  VARCHAR (50),
    OPERATOR_CODE  VARCHAR (50),
    SUBSCRIPTION_CHANNEL  VARCHAR (50),
    SERVICE_LIST  VARCHAR (50),
    SUBSCRIPTION_SERVICE  VARCHAR (50),
    SUBSCRIPTION_SERVICE_DETAILS  VARCHAR (50),
    SUBSCRIPTION_RELATED_SERVICE  VARCHAR (50),
    RATED_AMOUNT    FLOAT,
    MAIN_BALANCE_USED  VARCHAR (10),
    ACTIVE_DATE  DATE,
    ACTIVE_TIME  VARCHAR (6),
    EXPIRE_DATE  DATE,
    EXPIRE_TIME  VARCHAR (6),
    SUBSCRIPTION_STATUS  VARCHAR (50),
    PREVIOUS_COMMERCIAL_OFFER  VARCHAR (50),
    PREVIOUS_STATUS  VARCHAR (50),
    PREVIOUS_SUBS_SERVICE_DETAILS  VARCHAR (50),
    PREVIOUS_SUBS_RELATED_SERVICE  VARCHAR (50),
    TERMINATION_INDICATOR  VARCHAR (50),
    BENEFIT_BALANCE_LIST  VARCHAR (1000),
    BENEFIT_UNIT_LIST  VARCHAR (255),
    BENEFIT_ADDED_VALUE_LIST  VARCHAR (1000),
    BENEFIT_RESULT_VALUE_LIST  VARCHAR (1000),
    BENEFIT_ACTIVE_DATE_LIST  VARCHAR (2000),
    BENEFIT_EXPIRE_DATE_LIST  VARCHAR (2000),
    TOTAL_OCCURENCE  INT,
    INSERT_DATE  TIMESTAMP,
    SOURCE_INSERT_DATE  DATE,
    ORIGINAL_FILE_NAME  VARCHAR (100),
    SERVICE_CODE  VARCHAR (50),
    AMOUNT_VOICE_ONNET  FLOAT,
    AMOUNT_VOICE_OFFNET  FLOAT,
    AMOUNT_VOICE_INTER  FLOAT,
    AMOUNT_VOICE_ROAMING  FLOAT,
    AMOUNT_SMS_ONNET  FLOAT,
    AMOUNT_SMS_OFFNET  FLOAT,
    AMOUNT_SMS_INTER  FLOAT,
    AMOUNT_SMS_ROAMING  FLOAT,
    AMOUNT_DATA  FLOAT,
    AMOUNT_SVA  FLOAT,
    COMBO VARCHAR (10),
    benefit_bal_list STRING
)
COMMENT 'ZTE Subscriptions - FT'
PARTITIONED BY (TRANSACTION_DATE    DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')