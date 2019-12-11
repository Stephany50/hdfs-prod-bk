CREATE EXTERNAL TABLE CDR.tt_gesflot (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  CUSTID  VARCHAR(200),
  CUSTNAME  VARCHAR(200),
  PHONENUMBER  VARCHAR(200),
  EMAIL  VARCHAR(200),
  FAXNUMBER  VARCHAR(200))
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/ZSMART/GESTFLOT_'
TBLPROPERTIES ('serialization.null.format'='')
;