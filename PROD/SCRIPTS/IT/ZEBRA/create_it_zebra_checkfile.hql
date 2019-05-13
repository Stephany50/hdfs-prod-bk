---***********************************************************---
------------ IT ZEBRA CHECKFILE -------------------
----  Arnold Chuenffo 06-02-2019
---***********************************************************---

CREATE TABLE CDR.IT_ZEBRA_CHECKFILE (

  GENERATED_TIME  TIMESTAMP, 
  CDR_NUMBER  VARCHAR(100),
  CDR_NAME  VARCHAR(300),
  CDR_TOTAL_RECORDS  INT,
  ORIGINAL_FILE_NAME  VARCHAR(300),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ORIGINAL_FILE_DATE  DATE,
  INSERT_DATE  TIMESTAMP,
  CDR_DATE DATE
)
CLUSTERED BY(CDR_DATE) INTO 1 BUCKETS
STORED AS ORC 
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;
