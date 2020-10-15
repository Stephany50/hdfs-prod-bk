CREATE TABLE CDR.SPARK_IT_OM_TRANSACTION_ROLLBACK (
INPUT_TXNID VARCHAR(255),
OUTPUT_MESSAGE VARCHAR(255),
OUTPUT_TXNID VARCHAR(255),
OUTPUT_TXNSTATUS VARCHAR(255),
ROLLBACK_DATE VARCHAR(25),
ROLLBACK_STATUS VARCHAR(255),
SUBSCRIBER_MSISDN VARCHAR(255),
TXN_AMOUNT DOUBLE,
CHANNEL_MSISDN VARCHAR(255),
SOURCE VARCHAR(255),
ORIGINAL_FILE_NAME VARCHAR(50),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')