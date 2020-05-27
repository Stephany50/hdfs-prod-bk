CREATE TABLE CDR.SPARK_IT_CONT
(
    SUBS_ID VARCHAR(200),
    PARENT_ACCOUNT_NUMBER VARCHAR(200),
    ACCOUNT_NUMBER VARCHAR(200),
    RESP_PAYMENT VARCHAR(50),
    CUSTOMER_ID VARCHAR(200),
    SUBSCRIBER_TYPE  VARCHAR(200),
    DEFAULT_PRICE_PLAN_ID VARCHAR(200),
    ACCNBR VARCHAR(200),
    ICCID VARCHAR(200),
    IMSI VARCHAR(200),
    PROD_STATE VARCHAR(200),
    BLOCK_REASON VARCHAR(200),
    UPDATE_DATE TIMESTAMP,
    CUID VARCHAR(200),
    ACTIVATION_DATE TIMESTAMP,
    DATE_MAJ VARCHAR(200),
    LOGIN_UTILISATEUR_MAJ VARCHAR(200),
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE VARCHAR(200),
    ORIGINAL_FILE_LINE_COUNT VARCHAR(200),
    INSERT_DATE TIMESTAMP,
    ORDER_REASON VARCHAR(200)
)
    PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
    CLUSTERED BY(Subs_id) INTO 64 BUCKETS
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY");