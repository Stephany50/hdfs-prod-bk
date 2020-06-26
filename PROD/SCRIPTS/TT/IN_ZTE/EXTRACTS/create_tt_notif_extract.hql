CREATE EXTERNAL TABLE CDR.tt_zte_notif_extract (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  NOTIF_TYPE VARCHAR(400),
  DESCRIPTION VARCHAR(1000),
  MSISDN VARCHAR(60),
  MESSAGE VARCHAR(4000),
  CREATED_DATE DATE,
  SUBMITTED_DATE DATE
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/NOTIF_EXTRACT'
TBLPROPERTIES ('serialization.null.format'='');