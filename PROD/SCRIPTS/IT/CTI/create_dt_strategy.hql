
CREATE TABLE CTI.DT_STRATEGY (
    STRATEGY_KEY         INT,
    TENANT_KEY           INT,
    CREATE_AUDIT_KEY     BIGINT,
    UPDATE_AUDIT_KEY     BIGINT,
    STRATEGY_TYPE               VARCHAR(255),
    STRATEGY_TYPE_CODE          VARCHAR(32),
    STRATEGY_NAME               VARCHAR(255),
    PURGE_FLAG                  INT,
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_DATE DATE,
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
)
CLUSTERED BY(STRATEGY_KEY,ORIGINAL_FILE_DATE) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
