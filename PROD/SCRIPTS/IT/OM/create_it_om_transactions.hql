CREATE TMP.IT_OM_TRANSACTIONS (
  SENDER_MSISDN,
  RECEIVER_MSISDN,
  RECEIVER_USER_ID,
  SENDER_USER_ID,
  TRANSACTION_AMOUNT,
  COMMISSIONS_PAID,
  COMMISSIONS_RECEIVED,
  COMMISSIONS_OTHERS,
  SERVICE_CHARGE_RECEIVED,
  SERVICE_CHARGE_PAID,
  TAXES,
  SERVICE_TYPE,
  TRANSFER_STATUS,
  SENDER_PRE_BAL,
  SENDER_POST_BAL,
  RECEIVER_PRE_BAL,
  RECEIVER_POST_BAL,
  SENDER_ACC_STATUS,
  RECEIVER_ACC_STATUS,
  ERROR_CODE,
  ERROR_DESC,
  REFERENCE_NUMBER,
  CREATED_ON,
  CREATED_BY,
  MODIFIED_ON,
  MODIFIED_BY,
  APP_1_DATE,
  APP_2_DATE,
  TRANSFER_ID,
  TRANSFER_DATETIME,
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
  PAYMENT_DATE,
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
  ORIGINAL_FILE_SIZE,
  ORIGINAL_FILE_LINE_COUNT,
  ORIGINAL_FILE_DATE,
  INSERT_DATE
)
PARTITIONED (TRANSACTION_DATE
CLUSTEREDTRANSFER_ID) INTO BUCKETS
STORED ORC 
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");



INSERT INTO CDR.IT_OM_TRANSACTIONS SELECT
SENDER_MSISDN,
RECEIVER_MSISDN,
RECEIVER_USER_ID,
SENDER_USER_ID,
TRANSACTION_AMOUNT,
COMMISSIONS_PAID,
COMMISSIONS_RECEIVED,
COMMISSIONS_OTHERS,
SERVICE_CHARGE_RECEIVED,
SERVICE_CHARGE_PAID,
TAXES,
SERVICE_TYPE,
TRANSFER_STATUS,
SENDER_PRE_BAL,
SENDER_POST_BAL,
RECEIVER_PRE_BAL,
RECEIVER_POST_BAL,
SENDER_ACC_STATUS,
RECEIVER_ACC_STATUS,
ERROR_CODE,
ERROR_DESC,
REFERENCE_NUMBER,
CREATED_ON,
CREATED_BY,
MODIFIED_ON,
MODIFIED_BY,
APP_1_DATE,
APP_2_DATE,
TRANSFER_ID,
TRANSFER_DATETIME,
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
PAYMENT_DATE,
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
null ORIGINAL_FILE_SIZE,
null ORIGINAL_FILE_LINE_COUNT,
ORIGINAL_FILE_DATE,
INSERT_DATE,
to_date(TRANSFER_DATETIME) TRANSACTION_DATE
from backup_dwh.IT_OM_TRANSACTIONS





CREATE TABLE BACKUP_dwh.it_om_transactions (
    SENDER_MSISDN VARCHAR(100),
    RECEIVER_MSISDN VARCHAR(100),
    RECEIVER_USER_ID VARCHAR(100),
    SENDER_USER_ID VARCHAR(100),
    TRANSACTION_AMOUNT VARCHAR(100),
    COMMISSIONS_PAID VARCHAR(100),
    COMMISSIONS_RECEIVED VARCHAR(100),
    COMMISSIONS_OTHERS VARCHAR(100),
    SERVICE_CHARGE_RECEIVED VARCHAR(100),
    SERVICE_CHARGE_PAID VARCHAR(100),
    TAXES VARCHAR(100),
    SERVICE_TYPE VARCHAR(100),
    TRANSFER_STATUS VARCHAR(100),
    SENDER_PRE_BAL VARCHAR(100),
    SENDER_POST_BAL VARCHAR(100),
    RECEIVER_PRE_BAL VARCHAR(100),
    RECEIVER_POST_BAL VARCHAR(100),
    SENDER_ACC_STATUS VARCHAR(100),
    RECEIVER_ACC_STATUS VARCHAR(100),
    ERROR_CODE VARCHAR(100),
    ERROR_DESC VARCHAR(100),
    REFERENCE_NUMBER VARCHAR(100),
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
    PAYMENT_TYPE VARCHAR(100),
    PAYMENT_NUMBER VARCHAR(100),
    PAYMENT_DATE VARCHAR(100),
    REMARKS VARCHAR(100),
    ACTION_TYPE VARCHAR(100),
    TRANSACTION_TAG VARCHAR(100),
    RECONCILIATION_BY VARCHAR(100),
    RECONCILIATION_FOR VARCHAR(100),
    EXT_TXN_NUMBER VARCHAR(100),
    ORIGINAL_REF_NUMBER VARCHAR(100),
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
    BULK_PAYOUT_BATCHID VARCHAR(100),
    IS_FINANCIAL VARCHAR(100),
    TRANSFER_DONE VARCHAR(100),
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_DATE VARCHAR(100),
    INSERT_DATE VARCHAR(100),
    INITIATOR_MSISDN VARCHAR(100),
    VALIDATOR_MSISDN VARCHAR(100),
    INITIATOR_COMMENTS VARCHAR(100),
    VALIDATOR_COMMENTS VARCHAR(100),
    SENDER_WALLET_NAME VARCHAR(100),
    RECIEVER_WALLET_NAME VARCHAR(100),
    SENDER_USER_TYPE VARCHAR(100),
    RECEIVER_USER_TYPE VARCHAR(100)
)STORED AS ORC