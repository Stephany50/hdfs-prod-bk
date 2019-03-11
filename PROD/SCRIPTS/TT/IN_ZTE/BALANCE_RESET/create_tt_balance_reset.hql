---***********************************************************---
------------ External Table- BALANCE_RESET -------------------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_ZTE_BALANCE_RESET (
  ACC_NBR VARCHAR(16),
  ACCT_CODE VARCHAR(16),
  BAL_RESET_TIME TIMESTAMP,
  PRE_BALANCE VARCHAR(200),
  PROVIDER_ID INT,
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'Balance Reset external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/BALANCE_RESET/'
TBLPROPERTIES ('serialization.null.format'='')
;
