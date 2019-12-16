CREATE EXTERNAL TABLE CDR.tt_omny_operator_balance (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  MSISDN  VARCHAR(25),
  USER_ID  VARCHAR(100),
  LAST_TRANSFER_ID  VARCHAR(100),
  LAST_TRANSFER_ON  VARCHAR(20),
  BALANCE  DECIMAL(17,2),
  WALLET_SEQUENCE_NUMBER  DECIMAL(17,2),
  USER_TYPE  VARCHAR(25),
  STATUS  VARCHAR(5)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/ACCOUNTS_WITHOUT_MSISDN'
TBLPROPERTIES ('serialization.null.format'='')
;