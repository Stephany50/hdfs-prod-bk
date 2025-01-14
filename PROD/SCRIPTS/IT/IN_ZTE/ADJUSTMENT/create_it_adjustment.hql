---***********************************************************---
------------ CREATE IT Table- ADJUSTMENT -------------------
---***********************************************************---

CREATE TABLE CDR.IT_ZTE_ADJUSTMENT (
  ACCT_CODE VARCHAR(16),
  ACC_NBR VARCHAR(30),
  ACCT_BOOK_ID BIGINT,
  ACCT_RES_CODE VARCHAR(16),
  PRE_REAL_BALANCE BIGINT,
  CHARGE BIGINT,
  PRE_EXP_DATE TIMESTAMP,
  DAYS INT,
  CHANNEL_ID INT,
  NQ_CREATE_DATE TIMESTAMP,
  TRANSACTIONSN VARCHAR(25),
  PROVIDER_ID INT,
  PREPAY_FLAG INT,
  LOAN_AMOUNT BIGINT,
  COMMISSION_AMOUNT BIGINT,
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ORIGINAL_FILE_DATE DATE,
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (CREATE_DATE DATE)
CLUSTERED BY(ACCT_CODE,ACCT_RES_CODE,PROVIDER_ID) INTO 5 BUCKETS
STORED AS ORC 
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;
