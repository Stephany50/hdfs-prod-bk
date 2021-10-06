CREATE TABLE CDR.SPARK_IT_BILL_DELIVERY
(
    account_code VARCHAR(200),
    msisdn VARCHAR(32),
    customer_name VARCHAR(200),
    customer_category VARCHAR(64),
    bill_amount DECIMAL(32,8),
    account_status VARCHAR(32),
    email_address VARCHAR(64),
    delivery_method VARCHAR(32),
    delivery_statut VARCHAR(32),
    failed_reason VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(100),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (bill_month DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



 select * from CDR.SPARK_TT_BILL_DELIVERY limit 2;

CREATE EXTERNAL TABLE CDR.SPARK_TT_BILL_DELIVERY
(   
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    account_code VARCHAR(200),
    bill_month DATE,
    msisdn VARCHAR(32),
    customer_name VARCHAR(200),
    customer_category VARCHAR(64),
    bill_amount DECIMAL,
    account_status VARCHAR(32),
    email_address VARCHAR(64),
    delivery_method VARCHAR(32),
    delivery_statut VARCHAR(32),
    failed_reason VARCHAR(200)
)COMMENT 'CDR TT_AGING_BALANCE external table'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
LOCATION '/PROD/TT/ZSMART/BILL_DELIVERY'
TBLPROPERTIES ('serialization.null.format'='');