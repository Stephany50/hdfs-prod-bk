---***********************************************************---
------------    TT EMERGENCY_CREDIT   -------------------
---- ARNOLD CHUENFFO 05-02-2019
---***********************************************************---


CREATE EXTERNAL TABLE CDR.tt_zte_emergency_credit (
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  MSISDN  VARCHAR(20),
  TRANSACTION_DATE  TIMESTAMP,
  AMOUNT  BIGINT,
  TRANSACTION_TYPE  VARCHAR(20),
  FEE  BIGINT,
  CONTACT_CHANNEL  VARCHAR(20)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/IN_ZTE/EMERGENCY_CREDIT/'
TBLPROPERTIES ('serialization.null.format'='')
;

