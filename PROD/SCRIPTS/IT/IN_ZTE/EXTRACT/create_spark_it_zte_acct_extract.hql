CREATE TABLE CDR.SPARK_IT_ZTE_ACCT_EXTRACT (
  ACCT_ID  INT,
  CUST_ID  INT,
  SP_ID  INT,
  ROUTING_ID  INT,
  POSTPAID  CHAR (1),
  STATE_DATE  TIMESTAMP,
  STATE  CHAR (1),
  UPDATE_DATE  TIMESTAMP,
  CREATED_DATE  TIMESTAMP,
  BANK_ACCT_NAME  VARCHAR(120),
  BANK_ACCT_NBR  VARCHAR(60),
  BANK_ID  INT,
  PAYMENT_TYPE  CHAR (1),
  BILLING_CYCLE_TYPE_ID  INT,
  ACCT_NBR  VARCHAR(60),
  BILL_FORMAT_ID  INT,
  ORIGINAL_FILE_NAME VARCHAR(50),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(ACCT_ID) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
