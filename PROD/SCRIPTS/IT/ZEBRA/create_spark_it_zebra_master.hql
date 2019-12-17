---***********************************************************---
------------ IT ZEBRA MASTER -------------------
----  Arnold Chuenffo 06-02-2019
---***********************************************************---

CREATE TABLE CDR.SPARK_IT_ZEBRA_MASTER (

  CHANNEL_USER_ID  VARCHAR(20),
  PARENT_USER_ID  VARCHAR(20),
  OWNER_USER_ID  VARCHAR(20),
  USER_TYPE  VARCHAR(20),
  EXTERNAL_CODE  VARCHAR(20),
  PRIMARY_MSISDN  VARCHAR(20),
  USER_STATUS  VARCHAR(20),
  LOGIN_ID  VARCHAR(25),
  CATEGORY_CODE  VARCHAR(15),
  CATEGORY_NAME  VARCHAR(50),
  GEOGRAPHICAL_DOMAIN_CODE  VARCHAR(20),
  GEOGRAPHICAL_DOMAIN_NAME  VARCHAR(50),
  CHANNEL_USER_NAME  VARCHAR(80),
  CITY  VARCHAR(40),
  STATE  VARCHAR(20),
  COUNTRY  VARCHAR(40),
  ORIGINAL_FILE_NAME  VARCHAR(200),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ORIGINAL_FILE_DATE DATE,
  INSERT_DATE  TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE,FILE_DATE DATE)
CLUSTERED BY(GEOGRAPHICAL_DOMAIN_CODE) INTO 4 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')