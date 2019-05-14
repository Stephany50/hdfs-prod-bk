CREATE TABLE IF NOT EXISTS CTI.FT_SM_RES_SESSION_FACT (
    SM_RES_SESSION_FACT_KEY VARCHAR(50),
    START_DATE_TIME_KEY VARCHAR(50),
    END_DATE_TIME_KEY VARCHAR(50),
    TENANT_KEY VARCHAR(50),
    MEDIA_TYPE_KEY VARCHAR(50),
    RESOURCE_KEY VARCHAR(50),
    RESOURCE_GROUP_COMBINATION_KEY VARCHAR(50),
    CREATE_AUDIT_KEY VARCHAR(50),
    UPDATE_AUDIT_KEY VARCHAR(50),
    START_TS VARCHAR(50),
    END_TS VARCHAR(50),
    TOTAL_DURATION VARCHAR(50),
    LEAD_CLIP_DURATION VARCHAR(50),
    TRAIL_CLIP_DURATION VARCHAR(50),
    ACTIVE_FLAG VARCHAR(50),
    PURGE_FLAG VARCHAR(50),
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
)
CLUSTERED BY(ORIGINAL_FILE_NAME) INTO 4 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");