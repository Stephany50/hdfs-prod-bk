
CREATE TABLE CTI.DT_ROUTING_TARGET (
    ROUTING_TARGET_KEY         INT,
    TENANT_KEY                 INT,
    CREATE_AUDIT_KEY           BIGINT,
    UPDATE_AUDIT_KEY           BIGINT,
    ROUTING_TARGET_TYPE               VARCHAR(64),
    ROUTING_TARGET_TYPE_CODE          VARCHAR(64),
    TARGET_OBJECT_SELECTED            VARCHAR(255),
    AGENT_GROUP_NAME                  VARCHAR(255),
    PLACE_GROUP_NAME                  VARCHAR(255),
    SKILL_EXPRESSION                  VARCHAR(255),
    PURGE_FLAG                        INT,
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_DATE DATE,
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    INSERT_DATE TIMESTAMP
)
CLUSTERED BY(ROUTING_TARGET_KEY,ORIGINAL_FILE_DATE) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
