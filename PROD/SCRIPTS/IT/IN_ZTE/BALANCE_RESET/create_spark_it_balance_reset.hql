---***********************************************************---
------------ CREATE IT Table- BALANCE_RESET -------------------
---***********************************************************---

CREATE TABLE CDR.IT_ZTE_BALANCE_RESET (
  ACC_NBR VARCHAR(16),
  ACCT_CODE VARCHAR(16),
  BAL_RESET_TIME TIMESTAMP,
  PRE_BALANCE VARCHAR(200),
  PROVIDER_ID INT,
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ORIGINAL_FILE_DATE DATE,
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (BAL_RESET_DATE DATE,FILE_DATE DATE)
CLUSTERED BY(ACCT_CODE,PROVIDER_ID) INTO 5 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
