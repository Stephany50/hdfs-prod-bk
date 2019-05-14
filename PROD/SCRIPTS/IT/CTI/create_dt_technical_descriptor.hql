
CREATE TABLE CTI.DT_TECHNICAL_DESCRIPTOR (
    TECHNICAL_DESCRIPTOR_KEY   INT,
    TECHNICAL_RESULT                  VARCHAR(255),
    TECHNICAL_RESULT_CODE             VARCHAR(32),
    RESULT_REASON                     VARCHAR(255),
    RESULT_REASON_CODE                VARCHAR(32),
    RESOURCE_ROLE                     VARCHAR(255),
    RESOURCE_ROLE_CODE                VARCHAR(32),
    ROLE_REASON                       VARCHAR(255),
    ROLE_REASON_CODE                  VARCHAR(32),
    CREATE_AUDIT_KEY           BIGINT,
    UPDATE_AUDIT_KEY           BIGINT,
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_DATE DATE,
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
)
CLUSTERED BY(TECHNICAL_DESCRIPTOR_KEY,ORIGINAL_FILE_DATE) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
