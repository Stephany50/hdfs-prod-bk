CREATE TABLE CDR.SPARK_IT_BILL_PER_ACCOUNT
(
    BILL_TYPE_NAME VARCHAR(200),
    CREATED_BY VARCHAR(200),
    CYCLE_TYPE VARCHAR(200),
    INVOICE_DATE VARCHAR(200),
    GENERATION_DATE VARCHAR(200),
    ACCT_NBR VARCHAR(200),
    CUST_CATEGORY VARCHAR(200),
    CUST_SEGMENT VARCHAR(200),
    SMS_NBR VARCHAR(200),
    EMAIL VARCHAR(200),
    CC_EMAIL VARCHAR(200),
    CUST_CODE VARCHAR(200),
    DEPT_NAME VARCHAR(200),
    CUST_NAME VARCHAR(200),
    BILL_NUMBER VARCHAR(200),
    BILL_AMOUNT VARCHAR(200),
    BILL_OPEN_AMOUNT VARCHAR(200),
    PRE_BALANCE VARCHAR(200),
    ENCAISSEMENTS VARCHAR(200),
    ACCOUNT_CURRENT_BALANCE VARCHAR(200),
    PREVIOUS_BILL_DUE VARCHAR(200),
    VARIATION VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(200),
    INSERT_DATE TIMESTAMP,
    ORIGINAL_FILE_DATE  DATE

)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");



CREATE EXTERNAL TABLE CDR.SPARK_TT_BILL_PER_ACCOUNT
(
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    BILL_TYPE_NAME VARCHAR(200),
    CREATED_BY VARCHAR(200),
    CYCLE_TYPE VARCHAR(200),
    INVOICE_DATE VARCHAR(200),
    GENERATION_DATE VARCHAR(200),
    ACCT_NBR VARCHAR(200),
    CUST_CATEGORY VARCHAR(200),
    CUST_SEGMENT VARCHAR(200),
    SMS_NBR VARCHAR(200),
    EMAIL VARCHAR(200),
    CC_EMAIL VARCHAR(200),
    CUST_CODE VARCHAR(200),
    DEPT_NAME VARCHAR(200),
    CUST_NAME VARCHAR(200),
    BILL_NUMBER VARCHAR(200),
    BILL_AMOUNT VARCHAR(200),
    BILL_OPEN_AMOUNT VARCHAR(200),
    PRE_BALANCE VARCHAR(200),
    ENCAISSEMENTS VARCHAR(200),
    ACCOUNT_CURRENT_BALANCE VARCHAR(200),
    PREVIOUS_BILL_DUE VARCHAR(200),
    VARIATION VARCHAR(200)

)COMMENT 'CDR SPARK_TT_BILL_PER_ACCOUNT'
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ";"
)
LOCATION '/PROD/TT/STAT_TOOLS/DATAMARTB2B/BILL_PER_ACCOUNT'
TBLPROPERTIES ('serialization.null.format'='');