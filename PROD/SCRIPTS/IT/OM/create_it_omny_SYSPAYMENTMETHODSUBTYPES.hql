
CREATE TABLE CDR.IT_OMNY_SYSPAYMENTMETHODSUBTYPES (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
PAYMENT_METHOD_SUBTYPE_ID        VARCHAR(10),
PAYMENT_METHOD_TYPE_ID            VARCHAR(10),
SUBTYPE_NAME                       VARCHAR(50),
STATUS                               VARCHAR(5),
PAYMENT_TYPE_ID                       VARCHAR(5),
CREATED_BY                           VARCHAR(30),
CREATED_ON                           VARCHAR(10),
MODIFIED_BY                           VARCHAR(30),
MODIFIED_ON                           VARCHAR(10),
INSERT_DATE          TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(PAYMENT_TYPE_ID) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
