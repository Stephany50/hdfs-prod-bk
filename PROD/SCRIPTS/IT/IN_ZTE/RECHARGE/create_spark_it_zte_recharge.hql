
CREATE TABLE CDR.SPARK_IT_ZTE_RECHARGE (
    PAYMENT_ID BIGINT,
    ACCT_CODE VARCHAR(16),
    ACC_NBR VARCHAR(16),
    PAY_TIME TIMESTAMP,
    ACCT_RES_CODE VARCHAR(30),
    BILL_AMOUNT	BIGINT,
    RESULT_BALANCE BIGINT,
    CHANNEL_ID INT,
    PAYMENT_METHOD INT,
    DAYS INT,
    OLD_EXP_DATE TIMESTAMP,
    NEW_EXP_DATE TIMESTAMP,
    BENEFIT_NAME VARCHAR(200),
    BENEFIT_BAL_LIST VARCHAR(500),
    BENEFIT_PRICEPLAN VARCHAR(200),
    TRANSACTIONSN VARCHAR(30),
    PROVIDER_ID INT,
    PREPAY_FLAG INT,
    LOAN_AMOUNT BIGINT,
    COMMISSION_AMOUNT BIGINT,
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_DATE DATE,
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (PAY_DATE DATE,FILE_DATE DATE)
CLUSTERED BY(ACCT_CODE,ACCT_RES_CODE,CHANNEL_ID,PROVIDER_ID) INTO 5 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')