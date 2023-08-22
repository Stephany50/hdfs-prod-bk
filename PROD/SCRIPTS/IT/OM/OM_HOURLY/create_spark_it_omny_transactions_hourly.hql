CREATE EXTERNAL TABLE CDR.SPARK_TT_OMNY_TRANSACTIONS_HOURLY (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    SENDER_MSISDN VARCHAR(100),
    RECEIVER_MSISDN VARCHAR(100),
    RECEIVER_USER_ID VARCHAR(100),
    SENDER_USER_ID VARCHAR(100),
    TRANSACTION_AMOUNT DECIMAL(17,2),
    COMMISSIONS_PAID DECIMAL(17,2),
    COMMISSIONS_RECEIVED DECIMAL(17,2),
    COMMISSIONS_OTHERS DECIMAL(17,2),
    SERVICE_CHARGE_RECEIVED DECIMAL(17,2),
    SERVICE_CHARGE_PAID DECIMAL(17,2),
    TAXES DECIMAL(17,2),
    SERVICE_TYPE VARCHAR(100),
    TRANSFER_STATUS VARCHAR(100),
    SENDER_PRE_BAL DECIMAL(17,2),
    SENDER_POST_BAL DECIMAL(17,2),
    RECEIVER_PRE_BAL DECIMAL(17,2),
    RECEIVER_POST_BAL DECIMAL(17,2),
    SENDER_ACC_STATUS VARCHAR(100),
    RECEIVER_ACC_STATUS VARCHAR(100),
    ERROR_CODE VARCHAR(100),
    ERROR_DESC VARCHAR(500),
    REFERENCE_NUMBER VARCHAR(150),
    CREATED_ON VARCHAR(100),
    CREATED_BY VARCHAR(100),
    MODIFIED_ON VARCHAR(100),
    MODIFIED_BY VARCHAR(100),
    APP_1_DATE VARCHAR(100),
    APP_2_DATE VARCHAR(100),
    TRANSFER_ID VARCHAR(100),
    TRANSFER_DATETIME VARCHAR(100),
    SENDER_CATEGORY_CODE VARCHAR(100),
    SENDER_DOMAIN_CODE VARCHAR(100),
    SENDER_GRADE_NAME VARCHAR(100),
    SENDER_GROUP_ROLE VARCHAR(100),
    SENDER_DESIGNATION VARCHAR(100),
    SENDER_STATE VARCHAR(100),
    RECEIVER_CATEGORY_CODE VARCHAR(100),
    RECEIVER_DOMAIN_CODE VARCHAR(100),
    RECEIVER_GRADE_NAME VARCHAR(100),
    RECEIVER_GROUP_ROLE VARCHAR(100),
    RECEIVER_DESIGNATION VARCHAR(100),
    RECEIVER_STATE VARCHAR(100),
    SENDER_CITY VARCHAR(100),
    RECEIVER_CITY VARCHAR(100),
    APP_1_BY VARCHAR(100),
    APP_2_BY VARCHAR(100),
    REQUEST_SOURCE VARCHAR(100),
    GATEWAY_TYPE VARCHAR(100),
    TRANSFER_SUBTYPE VARCHAR(100),
    PAYMENT_TYPE VARCHAR(400),
    PAYMENT_NUMBER VARCHAR(400),
    PAYMENT_DATE VARCHAR(100),
    REMARKS VARCHAR(200),
    ACTION_TYPE VARCHAR(100),
    TRANSACTION_TAG VARCHAR(100),
    RECONCILIATION_BY VARCHAR(100),
    RECONCILIATION_FOR VARCHAR(100),
    EXT_TXN_NUMBER VARCHAR(100),
    ORIGINAL_REF_NUMBER VARCHAR(200),
    ZEBRA_AMBIGUOUS VARCHAR(100),
    ATTEMPT_STATUS VARCHAR(100),
    OTHER_MSISDN VARCHAR(100),
    SENDER_WALLET_NUMBER VARCHAR(100),
    RECEIVER_WALLET_NUMBER VARCHAR(100),
    SENDER_USER_NAME VARCHAR(100),
    RECEIVER_USER_NAME VARCHAR(100),
    TNO_MSISDN VARCHAR(100),
    TNO_ID VARCHAR(100),
    UNREG_FIRST_NAME VARCHAR(100),
    UNREG_LAST_NAME VARCHAR(100),
    UNREG_DOB VARCHAR(100),
    UNREG_ID_NUMBER VARCHAR(100),
    BULK_PAYOUT_BATCHID VARCHAR(400),
    IS_FINANCIAL VARCHAR(10),
    TRANSFER_DONE VARCHAR(10),
    INITIATOR_MSISDN VARCHAR(100),
    VALIDATOR_MSISDN VARCHAR(100),
    INITIATOR_COMMENTS VARCHAR(1000),
    VALIDATOR_COMMENTS VARCHAR(1000),
    SENDER_WALLET_NAME VARCHAR(100),
    RECIEVER_WALLET_NAME VARCHAR(100),
    SENDER_USER_TYPE VARCHAR(100),
    RECEIVER_USER_TYPE VARCHAR(100),
    TXNMODE VARCHAR(100)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM_HOURLY/CURRENT/TRANSACTIONS'
TBLPROPERTIES ('serialization.null.format'='')
;


CREATE TABLE CDR.SPARK_IT_OMNY_TRANSACTIONS_HOURLY
(
    SENDER_MSISDN VARCHAR(100),
    RECEIVER_MSISDN VARCHAR(100),
    RECEIVER_USER_ID VARCHAR(100),
    SENDER_USER_ID VARCHAR(100),
    TRANSACTION_AMOUNT DECIMAL(17,2),
    COMMISSIONS_PAID DECIMAL(17,2),
    COMMISSIONS_RECEIVED DECIMAL(17,2),
    COMMISSIONS_OTHERS DECIMAL(17,2),
    SERVICE_CHARGE_RECEIVED DECIMAL(17,2),
    SERVICE_CHARGE_PAID DECIMAL(17,2),
    TAXES DECIMAL(17,2),
    SERVICE_TYPE VARCHAR(100),
    TRANSFER_STATUS VARCHAR(100),
    SENDER_PRE_BAL DECIMAL(17,2),
    SENDER_POST_BAL DECIMAL(17,2),
    RECEIVER_PRE_BAL DECIMAL(17,2),
    RECEIVER_POST_BAL DECIMAL(17,2),
    SENDER_ACC_STATUS VARCHAR(100),
    RECEIVER_ACC_STATUS VARCHAR(100),
    ERROR_CODE VARCHAR(100),
    ERROR_DESC VARCHAR(500),
    REFERENCE_NUMBER VARCHAR(150),
    CREATED_ON TIMESTAMP,
    CREATED_BY VARCHAR(100),
    MODIFIED_ON TIMESTAMP,
    MODIFIED_BY VARCHAR(100),
    APP_1_DATE TIMESTAMP,
    APP_2_DATE TIMESTAMP,
    TRANSFER_ID VARCHAR(100),
    TRANSFER_DATETIME_NQ TIMESTAMP,
    SENDER_CATEGORY_CODE VARCHAR(100),
    SENDER_DOMAIN_CODE VARCHAR(100),
    SENDER_GRADE_NAME VARCHAR(100),
    SENDER_GROUP_ROLE VARCHAR(100),
    SENDER_DESIGNATION VARCHAR(100),
    SENDER_STATE VARCHAR(100),
    RECEIVER_CATEGORY_CODE VARCHAR(100),
    RECEIVER_DOMAIN_CODE VARCHAR(100),
    RECEIVER_GRADE_NAME VARCHAR(100),
    RECEIVER_GROUP_ROLE VARCHAR(100),
    RECEIVER_DESIGNATION VARCHAR(100),
    RECEIVER_STATE VARCHAR(100),
    SENDER_CITY VARCHAR(100),
    RECEIVER_CITY VARCHAR(100),
    APP_1_BY VARCHAR(100),
    APP_2_BY VARCHAR(100),
    REQUEST_SOURCE VARCHAR(100),
    GATEWAY_TYPE VARCHAR(100),
    TRANSFER_SUBTYPE VARCHAR(100),
    PAYMENT_TYPE VARCHAR(400),
    PAYMENT_NUMBER VARCHAR(400),
    PAYMENT_DATE TIMESTAMP,
    REMARKS VARCHAR(200),
    ACTION_TYPE VARCHAR(100),
    TRANSACTION_TAG VARCHAR(100),
    RECONCILIATION_BY VARCHAR(100),
    RECONCILIATION_FOR VARCHAR(100),
    EXT_TXN_NUMBER VARCHAR(100),
    ORIGINAL_REF_NUMBER VARCHAR(200),
    ZEBRA_AMBIGUOUS VARCHAR(100),
    ATTEMPT_STATUS VARCHAR(100),
    OTHER_MSISDN VARCHAR(100),
    SENDER_WALLET_NUMBER VARCHAR(100),
    RECEIVER_WALLET_NUMBER VARCHAR(100),
    SENDER_USER_NAME VARCHAR(100),
    RECEIVER_USER_NAME VARCHAR(100),
    TNO_MSISDN VARCHAR(100),
    TNO_ID VARCHAR(100),
    UNREG_FIRST_NAME VARCHAR(100),
    UNREG_LAST_NAME VARCHAR(100),
    UNREG_DOB VARCHAR(100),
    UNREG_ID_NUMBER VARCHAR(100),
    BULK_PAYOUT_BATCHID VARCHAR(400),
    IS_FINANCIAL VARCHAR(10),
    TRANSFER_DONE VARCHAR(10),
    INITIATOR_MSISDN VARCHAR(100),
    VALIDATOR_MSISDN VARCHAR(100),
    INITIATOR_COMMENTS VARCHAR(1000),
    VALIDATOR_COMMENTS VARCHAR(1000),
    SENDER_WALLET_NAME VARCHAR(100),
    RECIEVER_WALLET_NAME VARCHAR(100),
    SENDER_USER_TYPE VARCHAR(100),
    RECEIVER_USER_TYPE VARCHAR(100),
    TXNMODE VARCHAR(100),
    ORIGINAL_FILE_NAME  VARCHAR(50),
    ORIGINAL_FILE_SIZE  INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
   )
  PARTITIONED BY (FILE_DATE DATE, TRANSFER_DATETIME DATE)
  STORED AS PARQUET
  TBLPROPERTIES ("parquet.compress"="SNAPPY");