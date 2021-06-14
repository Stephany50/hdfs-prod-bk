CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_ZTE_LOAN_CDR (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  MSISDN  VARCHAR(200),
  TRANSACTION_DATE  VARCHAR(200),
  PRICE_PLAN_CODE VARCHAR(200),
  AMOUNT  BIGINT,
  TRANSACTION_TYPE  VARCHAR(160),
  CONTACT_CHANNEL  VARCHAR(160)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/LOAN_CDR'
TBLPROPERTIES ('serialization.null.format'='')
;