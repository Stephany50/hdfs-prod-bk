CREATE TABLE CDR.IT_OMNY_DELETED_BILLS (
ORIGINAL_FILE_NAME        VARCHAR(50),
ORIGINAL_FILE_SIZE        INT,
ORIGINAL_FILE_LINE_COUNT  INT,
BILLER_CODE               VARCHAR(10),
BILL_ACCOUNT_NUMBER       VARCHAR(50),
BILL_NUMBER               VARCHAR(20),
BILL_STATUS               VARCHAR(30),
AMOUNT                    DECIMAL(17,2),
BILL_DUE_DATE             TIMESTAMP,
DELETION_DATE_TIME        TIMESTAMP,
BILL_CREATED_ON           TIMESTAMP,
LAST_MODIFIED_ON          TIMESTAMP,
FILE_NAME_USED_FOR_UPLOAD  VARCHAR(250),
USER_ID                    VARCHAR(50),
INSERT_DATE                TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(BILLER_CODE) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");