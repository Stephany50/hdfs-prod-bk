CREATE TABLE    
(
    gl_code VARCHAR(32),
    marche VARCHAR(32),
    txn_no VARCHAR(64),
    txn_type_code VARCHAR(32),
    txn_type_name VARCHAR(64),
    pay_amount DECIMAL(32,8),
    cust_code VARCHAR(32),
    cust_type_name VARCHAR(64),
    acct_nbr VARCHAR(64),
    cust_name VARCHAR(64),
    invoice_date date,
    invoice_no VARCHAR(32),
    cust_category VARCHAR(32),
    payment_no VARCHAR(64),
    payment_method_name VARCHAR(64),
    check_no VARCHAR(64),
    credit_card_no VARCHAR(64),
    credit_note_no VARCHAR(64),
    bank_code_no VARCHAR(64),
    ORIGINAL_FILE_NAME VARCHAR(100),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (stat_date DATE) 
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");


CREATE EXTERNAL TABLE CDR.SPARK_TT_DAILY_PAYMENT
(
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    stat_date DATE,
    gl_code VARCHAR(32),
    marche VARCHAR(32),
    txn_no VARCHAR(64),
    txn_type_code VARCHAR(32),
    txn_type_name VARCHAR(64),
    pay_amount DECIMAL,
    cust_code VARCHAR(32),
    cust_type_name VARCHAR(64),
    acct_nbr VARCHAR(64),
    cust_name VARCHAR(64),
    invoice_date date,
    invoice_no VARCHAR(32),
    cust_category VARCHAR(32),
    payment_no VARCHAR(64),
    payment_method_name VARCHAR(64),
    check_no VARCHAR(64),
    credit_card_no VARCHAR(64),
    credit_note_no VARCHAR(64),
    bank_code_no VARCHAR(64)
)COMMENT 'CDR TABLE TT_DAILY_PAYMENT'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
WITH SERDEPROPERTIES (
   "separatorChar" = "|"
)
LOCATION '/PROD/TT/ZSMART/DAILY_PAYMENT'
TBLPROPERTIES ('serialization.null.format'='');