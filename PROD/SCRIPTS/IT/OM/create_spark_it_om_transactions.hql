CREATE TABLE junk.SPARK_IT_OMNY_TRANSACTIONS
   (
    SENDER_MSISDN VARCHAR(100),
  RECEIVER_MSISDN VARCHAR(100),
  RECEIVER_USER_ID VARCHAR(100),
  SENDER_USER_ID VARCHAR(100),
  TRANSACTION_AMOUNT ,
  COMMISSIONS_PAID ,
  COMMISSIONS_RECEIVED ,
  COMMISSIONS_OTHERS ,
  SERVICE_CHARGE_RECEIVED ,
  SERVICE_CHARGE_PAID ,
  TAXES ,
  SERVICE_TYPE VARCHAR(100),
  TRANSFER_STATUS VARCHAR(100),
  SENDER_PRE_BAL ,
  SENDER_POST_BAL ,
  RECEIVER_PRE_BAL ,
  RECEIVER_POST_BAL ,
  SENDER_ACC_STATUS VARCHAR(50),
  RECEIVER_ACC_STATUS VARCHAR(50),
  ERROR_CODE VARCHAR(50),
  ERROR_DESC VARCHAR(1000),
  REFERENCE_NUMBER VARCHAR(50),
  CREATED_ON ,
  CREATED_BY VARCHAR(50),
  MODIFIED_ON ,
  MODIFIED_BY VARCHAR(50),
  APP_1_DATE ,
  APP_2_DATE ,
  TRANSFER_ID VARCHAR(50),
  TRANSFER_DATETIME_NQ ,
  SENDER_CATEGORY_CODE VARCHAR(50),
  SENDER_DOMAIN_CODE VARCHAR(50),
  SENDER_GRADE_NAME VARCHAR(100),
  SENDER_GROUP_ROLE VARCHAR(50),
  SENDER_DESIGNATION VARCHAR(100),
  SENDER_STATE VARCHAR(100),
  RECEIVER_CATEGORY_CODE VARCHAR(50),
  RECEIVER_DOMAIN_CODE VARCHAR(50),
  RECEIVER_GRADE_NAME VARCHAR(100),
  RECEIVER_GROUP_ROLE VARCHAR(50),
  RECEIVER_DESIGNATION VARCHAR(100),
  RECEIVER_STATE VARCHAR(100),
  SENDER_CITY VARCHAR(50),
  RECEIVER_CITY VARCHAR(50),
  APP_1_BY VARCHAR(50),
  APP_2_BY VARCHAR(50),
  REQUEST_SOURCE VARCHAR(50),
  GATEWAY_TYPE VARCHAR(50),
  TRANSFER_SUBTYPE VARCHAR(100),
  PAYMENT_TYPE VARCHAR(100),
  PAYMENT_NUMBER VARCHAR(50),
  PAYMENT_DATE ,
  REMARKS VARCHAR(500),
  ACTION_TYPE VARCHAR(50),
  TRANSACTION_TAG VARCHAR(100),
  RECONCILIATION_BY VARCHAR(50),
  RECONCILIATION_FOR VARCHAR(50),
  EXT_TXN_NUMBER VARCHAR(50),
  ORIGINAL_REF_NUMBER VARCHAR(50),
  ZEBRA_AMBIGUOUS VARCHAR(50),
  ATTEMPT_STATUS VARCHAR(50),
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
  UNREG_ID_NUMBER VARCHAR(50),
  BULK_PAYOUT_BATCHID VARCHAR(50),
  IS_FINANCIAL VARCHAR(50),
  TRANSFER_DONE VARCHAR(50),
  INITIATOR_MSISDN VARCHAR(50),
  VALIDATOR_MSISDN VARCHAR(50),
  INITIATOR_COMMENTS VARCHAR(1000),
  VALIDATOR_COMMENTS VARCHAR(1000),
  SENDER_WALLET_NAME VARCHAR(50),
  RECIEVER_WALLET_NAME VARCHAR(50),
  SENDER_USER_TYPE VARCHAR(50),
  RECEIVER_USER_TYPE VARCHAR(50),
  ORIGINAL_FILE_NAME VARCHAR(50),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
  ORIGINAL_FILE_DATE DATE,
  INSERT_DATE 
)
PARTITIONED BY (TRANSFER_DATETIME DATE, FILE_DATE DATE)
CLUSTERED BY(SENDER_MSISDN) INTO 64 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')



insert into junk.SPARK_IT_OMNY_TRANSACTIONS
SELECT
SENDER_MSISDN,
RECEIVER_MSISDN,
RECEIVER_USER_ID,
SENDER_USER_ID,
TRANSACTION_AMOUNT ,
COMMISSIONS_PAID ,
COMMISSIONS_RECEIVED ,
COMMISSIONS_OTHERS ,
SERVICE_CHARGE_RECEIVED ,
SERVICE_CHARGE_PAID ,
TAXES ,
SERVICE_TYPE,
TRANSFER_STATUS,
SENDER_PRE_BAL ,
SENDER_POST_BAL ,
RECEIVER_PRE_BAL ,
RECEIVER_POST_BAL ,
SENDER_ACC_STATUS,
RECEIVER_ACC_STATUS,
ERROR_CODE,
ERROR_DESC,
REFERENCE_NUMBER,
CREATED_ON ,
CREATED_BY,
MODIFIED_ON ,
MODIFIED_BY,
APP_1_DATE ,
APP_2_DATE ,
TRANSFER_ID,
TRANSFER_DATETIME TRANSFER_DATETIME_NQ ,
SENDER_CATEGORY_CODE,
SENDER_DOMAIN_CODE,
SENDER_GRADE_NAME,
SENDER_GROUP_ROLE,
SENDER_DESIGNATION,
SENDER_STATE,
RECEIVER_CATEGORY_CODE,
RECEIVER_DOMAIN_CODE,
RECEIVER_GRADE_NAME,
RECEIVER_GROUP_ROLE,
RECEIVER_DESIGNATION,
RECEIVER_STATE,
SENDER_CITY,
RECEIVER_CITY,
APP_1_BY,
APP_2_BY,
REQUEST_SOURCE,
GATEWAY_TYPE,
TRANSFER_SUBTYPE,
PAYMENT_TYPE,
PAYMENT_NUMBER,
PAYMENT_DATE ,
REMARKS,
ACTION_TYPE,
TRANSACTION_TAG,
RECONCILIATION_BY,
RECONCILIATION_FOR,
EXT_TXN_NUMBER,
ORIGINAL_REF_NUMBER,
ZEBRA_AMBIGUOUS,
ATTEMPT_STATUS,
OTHER_MSISDN,
SENDER_WALLET_NUMBER,
RECEIVER_WALLET_NUMBER,
SENDER_USER_NAME,
RECEIVER_USER_NAME,
TNO_MSISDN,
TNO_ID,
UNREG_FIRST_NAME,
UNREG_LAST_NAME,
UNREG_DOB,
UNREG_ID_NUMBER,
BULK_PAYOUT_BATCHID,
IS_FINANCIAL,
TRANSFER_DONE,
INITIATOR_MSISDN,
VALIDATOR_MSISDN,
INITIATOR_COMMENTS,
VALIDATOR_COMMENTS,
SENDER_WALLET_NAME,
RECIEVER_WALLET_NAME,
SENDER_USER_TYPE,
RECEIVER_USER_TYPE,
ORIGINAL_FILE_NAME,
NULL ORIGINAL_FILE_SIZE ,
NULL ORIGINAL_FILE_LINE_COUNT ,
to_date(ORIGINAL_FILE_DATE)  ORIGINAL_FILE_DATE,
INSERT_DATE,
to_date(TRANSFER_DATETIME) TRANSFER_DATETIME,
to_date(ORIGINAL_FILE_DATE) FILE_DATE
from backup_dwh.it_omny_transactions6