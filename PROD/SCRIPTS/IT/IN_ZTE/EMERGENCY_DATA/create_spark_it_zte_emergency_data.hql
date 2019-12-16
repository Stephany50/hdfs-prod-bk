CREATE TABLE CDR.SPARK_IT_ZTE_EMERGENCY_DATA (
    
 MSISDN	VARCHAR(20),
 TRANSACTION_TIME	VARCHAR(20), 
 AMOUNT	BIGINT,
 TRANSACTION_TYPE	VARCHAR(20),
 FEE	BIGINT,
 CONTACT_CHANNEL	VARCHAR(20),
 ORIGINAL_FILE_NAME VARCHAR(200),
 ORIGINAL_FILE_SIZE INT,
 ORIGINAL_FILE_LINE_COUNT INT,
 ORIGINAL_FILE_DATE DATE,
 INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
CLUSTERED BY(MSISDN) INTO 2 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')