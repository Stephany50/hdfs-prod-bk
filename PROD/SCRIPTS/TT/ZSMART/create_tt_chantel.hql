CREATE EXTERNAL TABLE CDR.tt_chantel (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  CUSTID  VARCHAR(200),
  SUBS_ID  VARCHAR(200),
  ACCNBR  VARCHAR(200),
  UPDATE_DATE  VARCHAR(200)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZSMART/CHANTEL_'
TBLPROPERTIES ('serialization.null.format'='')
;
