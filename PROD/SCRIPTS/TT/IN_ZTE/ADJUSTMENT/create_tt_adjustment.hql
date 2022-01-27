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
  PRE_EXP_DATE  VARCHAR(25),
  DAYS  INT,
  CHANNEL_ID  INT,
  CREATE_DATE  VARCHAR(25),
  TRANSACTIONSN  VARCHAR(25),
  PROVIDER_ID  INT,
  PREPAY_FLAG  INT,
  LOAN_AMOUNT  BIGINT,
  COMMISSION_AMOUNT  BIGINT,
  PARTY_TYPE  VARCHAR(2),
  STAFF_NAME VARCHAR(100),
  USER_CODE VARCHAR(25),
  LIST_BAL_ID VARCHAR(200)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/ADJUSTMENT'
TBLPROPERTIES ('serialization.null.format'='')
;

alter table CDR.tt_zte_adjustment add columns (PARTY_TYPE  VARCHAR(2), STAFF_NAME VARCHAR(100), USER_CODE VARCHAR(25)))

alter table CDR.tt_zte_adjustment
add columns (LIST_BAL_ID VARCHAR(200))
change pre_real_balance pre_real_balance varchar(100)
change pre_exp_date pre_exp_date varchar(100)
change days days varchar(20);
