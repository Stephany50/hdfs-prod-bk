CREATE EXTERNAL TABLE CDR.tt_zte_bal_snap (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  CREATE_DATE  VARCHAR(20),
  BAL_ID  BIGINT,
  ACCT_ID  BIGINT,
  ACCT_RES_ID  BIGINT,
  GROSS_BAL  DOUBLE,
  RESERVE_BAL  DOUBLE,
  CONSUME_BAL  DOUBLE,
  RATING_BAL  DOUBLE,
  BILLING_BAL  DOUBLE,
  EFF_DATE  VARCHAR(20),
  EXP_DATE  VARCHAR(20),
  UPDATE_DATE  VARCHAR(20),
  CEIL_LIMIT  DOUBLE,
  FLOOR_LIMIT  DOUBLE,
  DAILY_CEIL_LIMIT  DOUBLE,
  DAILY_FLOOR_LIMIT  DOUBLE,
  PRIORITY  DOUBLE,
  LAST_BAL  DOUBLE,
  LAST_RECHARGE  DOUBLE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/PROD/TT/IN_ZTE/BAL_SNAP'
TBLPROPERTIES ('serialization.null.format'='')
;