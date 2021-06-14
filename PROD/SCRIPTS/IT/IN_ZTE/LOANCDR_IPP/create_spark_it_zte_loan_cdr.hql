CREATE TABLE CDR.SPARK_IT_ZTE_LOAN_CDR (MSISDN  VARCHAR(200),
  PRICE_PLAN_CODE VARCHAR(200),
  AMOUNT  BIGINT,
  TRANSACTION_TYPE  VARCHAR(160),
  CONTACT_CHANNEL  VARCHAR(160),
  ORIGINAL_FILE_NAME VARCHAR(200),
  ORIGINAL_FILE_DATE DATE,
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')