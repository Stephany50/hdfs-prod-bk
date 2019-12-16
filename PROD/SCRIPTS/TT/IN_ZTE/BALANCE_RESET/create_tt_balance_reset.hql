---***********************************************************---
------------ External Table- BALANCE_RESET -------------------
---***********************************************************---

CREATE EXTERNAL TABLE CDR.tt_zte_balance_reset (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ACC_NBR  VARCHAR(16),
  ACCT_CODE  VARCHAR(16),
  BAL_RESET_TIME  TIMESTAMP,
  PRE_BALANCE  VARCHAR(200),
  PROVIDER_ID  INT
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/BALANCE_RESET'
TBLPROPERTIES ('serialization.null.format'='')
;
