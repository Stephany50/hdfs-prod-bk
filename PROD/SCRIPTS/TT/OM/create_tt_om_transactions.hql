CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_TRANSACTIONS (
ORIGINAL_FILE_NAME VARCHAR(50),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
SENDER_MSISDN	VARCHAR(50),
RECEIVER_MSISDN	VARCHAR(50),
RECEIVER_USER_ID	VARCHAR(50),
SENDER_USER_ID	VARCHAR(50),
TRANSACTION_AMOUNT	DECIMAL(17,2),
COMMISSIONS_PAID	DECIMAL(17,2),
COMMISSIONS_RECEIVED	DECIMAL(17,2),
COMMISSIONS_OTHERS	DECIMAL(17,2),
SERVICE_CHARGE_RECEIVED	DECIMAL(17,2),
SERVICE_CHARGE_PAID	DECIMAL(17,2),
TAXES	DECIMAL(17,2),
SERVICE_TYPE	VARCHAR(50),
TRANSFER_STATUS	VARCHAR(50),
SENDER_PRE_BAL	DECIMAL(17,2),
SENDER_POST_BAL	DECIMAL(17,2),
RECEIVER_PRE_BAL	DECIMAL(17,2),
RECEIVER_POST_BAL	DECIMAL(17,2),
SENDER_ACC_STATUS	VARCHAR(50),
RECEIVER_ACC_STATUS VARCHAR(50),
ERROR_CODE	VARCHAR(50),
ERROR_DESC	VARCHAR(350),
REFERENCE_NUMBER	VARCHAR(100),
CREATED_ON	VARCHAR(50),
CREATED_BY	VARCHAR(50),
MODIFIED_ON	VARCHAR(50),
MODIFIED_BY	VARCHAR(50),
APP_1_DATE	VARCHAR(50),
APP_2_DATE	VARCHAR(50),
TRANSFER_ID	VARCHAR(50),
TRANSFER_DATETIME	VARCHAR(50),
SENDER_CATEGORY_CODE	VARCHAR(50),
SENDER_DOMAIN_CODE	VARCHAR(50),
SENDER_GRADE_NAME	VARCHAR(50),
SENDER_GROUP_ROLE	VARCHAR(50),
SENDER_DESIGNATION	VARCHAR(50),
SENDER_STATE	VARCHAR(50),
RECEIVER_CATEGORY_CODE	VARCHAR(50),
RECEIVER_DOMAIN_CODE	VARCHAR(50),
RECEIVER_GRADE_NAME VARCHAR(50),
RECEIVER_GROUP_ROLE	VARCHAR(50),
RECEIVER_DESIGNATION	VARCHAR(50),
RECEIVER_STATE	VARCHAR(50),
SENDER_CITY	VARCHAR(50),
RECEIVER_CITY	VARCHAR(50),
APP_1_BY	VARCHAR(50),
APP_2_BY	VARCHAR(50),
REQUEST_SOURCE	VARCHAR(50),
GATEWAY_TYPE	VARCHAR(50),
TRANSFER_SUBTYPE	VARCHAR(50),
PAYMENT_TYPE	VARCHAR(255),
PAYMENT_NUMBER	VARCHAR(255),
PAYMENT_DATE	VARCHAR(50),
REMARKS	VARCHAR(100),
ACTION_TYPE	VARCHAR(50),
TRANSACTION_TAG	VARCHAR(50),
RECONCILIATION_BY	VARCHAR(50),
RECONCILIATION_FOR	VARCHAR(50),
EXT_TXN_NUMBER	VARCHAR(50),
ORIGINAL_REF_NUMBER	VARCHAR(100),
ZEBRA_AMBIGUOUS	VARCHAR(50),
ATTEMPT_STATUS	VARCHAR(20),
OTHER_MSISDN	VARCHAR(255),
SENDER_WALLET_NUMBER	VARCHAR(50),
RECEIVER_WALLET_NUMBER	VARCHAR(50),
SENDER_USER_NAME	VARCHAR(80),
RECEIVER_USER_NAME	VARCHAR(80),
TNO_MSISDN	VARCHAR(50),
TNO_ID	VARCHAR(50),
UNREG_FIRST_NAME	VARCHAR(80),
UNREG_LAST_NAME	VARCHAR(80),
UNREG_DOB	VARCHAR(50),
UNREG_ID_NUMBER VARCHAR(50),
BULK_PAYOUT_BATCHID	VARCHAR(255),
IS_FINANCIAL	VARCHAR(1),
TRANSFER_DONE	VARCHAR(1),
INITIATOR_MSISDN	VARCHAR(50),
VALIDATOR_MSISDN	VARCHAR(50),
INITIATOR_COMMENTS	VARCHAR(100),
VALIDATOR_COMMENTS	VARCHAR(100),
SENDER_WALLET_NAME	VARCHAR(50),
RECIEVER_WALLET_NAME	VARCHAR(20),
SENDER_USER_TYPE	VARCHAR(50),
RECEIVER_USER_TYPE	VARCHAR(50)

)
COMMENT 'OM Transactions'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/TRANSACTIONS'
TBLPROPERTIES ('serialization.null.format'='');
