---***********************************************************---
------------    TT ZEBRA CHECKFILE   -------------------
---- ARNOLD CHUENFFO 05-02-2019
---***********************************************************---


CREATE EXTERNAL TABLE CDR.tt_zebra_checkfile (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  CDR_DATE  VARCHAR(30),
  CDR_NUMBER  VARCHAR(100),
  CDR_NAME  VARCHAR(300),
  CDR_TOTAL_RECORDS  INT
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u003B'
LOCATION '/PROD/TT/ZEBRA/ZEBRA_CHECKFILE/'
TBLPROPERTIES ('serialization.null.format'='')
;
