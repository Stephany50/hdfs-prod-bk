---***********************************************************---
------------ External Table-TT ADJUSTMENT -------------------
---***********************************************************---

CREATE EXTERNAL TABLE CDR.tt_zte_adjustment (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ACCT_CODE  VARCHAR(16),
  ACC_NBR  VARCHAR(30),
  ACCT_BOOK_ID  BIGINT,
  ACCT_RES_CODE  VARCHAR(16),
  PRE_REAL_BALANCE  BIGINT,
  CHARGE  BIGINT,
  PRE_EXP_DATE  TIMESTAMP,
  DAYS  INT,
  CHANNEL_ID  INT,
  CREATE_DATE  TIMESTAMP,
  TRANSACTIONSN  VARCHAR(25),
  PROVIDER_ID  INT,
  PREPAY_FLAG  INT,
  LOAN_AMOUNT  BIGINT,
  COMMISSION_AMOUNT  BIGINT
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/ADJUSTMENT'
TBLPROPERTIES ('serialization.null.format'='')
;