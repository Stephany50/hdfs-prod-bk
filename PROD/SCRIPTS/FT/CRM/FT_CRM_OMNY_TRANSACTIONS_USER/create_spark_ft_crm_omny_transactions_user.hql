CREATE TABLE MON.SPARK_FT_CRM_OMNY_TRANSACTIONS_USER


(SENDER_MSISDN VARCHAR(100),
RECEIVER_MSISDN_ACC VARCHAR(100),
RECIEVER_USER_ID VARCHAR(100),
SENDER_USER_ID VARCHAR(100),
TRANSACTION_AMOUNT INT,
SERVICE_TYPE VARCHAR(100),
TRANSFER_STATUS VARCHAR(10),
SENDER_PRE_BAL DECIMAL(17,2),
SENDER_POST_BAL DECIMAL(17,2),
RECEIVER_PRE_BAL DECIMAL(17,2),
RECEIVER_POST_BAL DECIMAL(17,2),
SENDER_ACC_STATUS VARCHAR(5),
RECEIVER_ACC_STATUS VARCHAR(5),
ERROR_CODE VARCHAR(25),
ERROR_DESC VARCHAR(1000),
REFERENCE_NUMBER VARCHAR(50),
REF_DATE DATE,
APP_1_DATE DATE,
APP_2_DATE DATE,
TRANSFER_ID VARCHAR(50),
TRANSFER_TIME VARCHAR(8),
SENDER_CATEGORY_CODE VARCHAR(50),
RECIEVER_CATEGORY_CODE VARCHAR(50),
SENDER_CITY VARCHAR(50),
RECEIVER_CITY VARCHAR(50),
ORIGINAL_FILE_NAME VARCHAR(100),
ORIGINAL_FILE_DATE DATE,
INSERT_DATE DATE,
CREATED_ON DATE,
CREATED_BY VARCHAR(50),
MODIFIED_ON DATE,
MODIFIED_BY VARCHAR(50),
COMMISSIONS_PAID DECIMAL(17,2),
COMMISSIONS_RECEIVED DECIMAL(17,2),
COMMISSIONS_OTHERS DECIMAL(17,2),
SERVICE_CHARGE_RECEIVED DECIMAL(17,2),
SERVICE_CHARGE_PAID DECIMAL(17,2)
)


PARTITIONED BY(TRANSFER_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")











