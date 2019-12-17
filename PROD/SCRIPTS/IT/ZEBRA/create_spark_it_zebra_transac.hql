---***********************************************************---
------------ IT_ZEBRA_TRANSAC -------------------------------
----  Arnold Chuenffo 06-02-2019
---***********************************************************---


CREATE TABLE CDR.SPARK_IT_ZEBRA_TRANSAC (

  TRANSFER_ID  VARCHAR(50),
  REQUEST_SOURCE  VARCHAR(20),
  TRANSFER_DATE_TIME  TIMESTAMP,
  NETWORK_CODE  VARCHAR(20),
  TRANSACTION_TYPE  VARCHAR(20),
  TRANSACTION_SUB_TYPE  VARCHAR(20),
  TRANSACTION_CATEGORY  VARCHAR(20),
  CHANNEL_TYPE  VARCHAR(20),
  FROM_USER_ID  VARCHAR(15),
  TO_USER_ID  VARCHAR(20),
  SENDER_MSISDN  VARCHAR(20),
  RECEIVER_MSISDN  VARCHAR(20),
  SENDER_CATEGORY  VARCHAR(20),
  RECEIVER_CATEGORY  VARCHAR(20),
  SENDER_DEBIT_AMOUNT  BIGINT,
  RECEIVER_CREDIT_AMOUNT   BIGINT,
  TRANSFER_AMOUNT   BIGINT,
  MRP   BIGINT,
  PAYABLE_AMOUNT   BIGINT,
  NET_PAYABLE_AMOUNT   BIGINT,
  RECEIVER_PROCESSING_FEE   BIGINT,
  RECEIVER_TAX1_AMOUNT   BIGINT,
  RECEIVER_TAX2_AMOUNT   BIGINT,
  RECEIVER_TAX3_AMOUNT   BIGINT,
  COMMISION   BIGINT,
  DIFFERENTIAL_APPLICABLE  VARCHAR(20),
  DIFFERENTIAL_GIVEN  VARCHAR(20),
  EXTERNAL_INVOICE_NUMBER  VARCHAR(25),
  EXTERNAL_INVOICE_DATE TIMESTAMP ,
  EXTERNAL_USER_ID  VARCHAR(25),
  PRODUCT  VARCHAR(25),
  CREDIT_BACK_STATUS  VARCHAR(15),
  TRANSFER_STATUS  VARCHAR(15),
  RECEIVER_BONUS   BIGINT,
  RECEIVER_VALIDITY   INT,
  RECEIVER_BONUS_VALIDITY   INT,
  SERVICE_CLASS_CODE  VARCHAR(15),
  INTERFACE_ID  VARCHAR(15),
  CARD_GROUP  VARCHAR(15),
  ERROR_REASON  VARCHAR(250),
  SERIAL_NUMBER  VARCHAR(30),
  IN_TXN  VARCHAR(30),
  SEND_PRE_BAL   BIGINT,
  SEND_POST_BAL   BIGINT,
  RCVR_PRE_BAL   BIGINT,
  RCVR_POST_BAL   BIGINT,
  TXN_WALLET  VARCHAR(30),
  ACTIVE_USER_ID  VARCHAR(30),
  BONUS_ACCOUNT_DETAILS  VARCHAR(150),
  TRANSFER_INITIATED_BY VARCHAR(15),
  FIRST_APPROVED_BY VARCHAR(15),
  SECOND_APPROVED_BY VARCHAR(15),
  OTHER_COMMISION VARCHAR(15),
  THIRD_APPROVED_BY VARCHAR(15),
  -- BONUS_MAIN   INT,
  -- BONUS_PROMO   INT,
  -- BONUS_SMS   INT,
  -- BONUS_VOICE   INT,
  ORIGINAL_FILE_NAME  VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ORIGINAL_FILE_DATE DATE,
  INSERT_DATE  TIMESTAMP
)
PARTITIONED BY (TRANSFER_DATE DATE,FILE_DATE DATE)
CLUSTERED BY(TRANSFER_ID) INTO 4 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')