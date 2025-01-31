
CREATE TABLE IF NOT EXISTS CTI.FT_IXN_RESOURCE_STATE_FACT (
    IXN_RESOURCE_STATE_FACT_KEY  BIGINT,
    START_DATE_TIME_KEY  BIGINT,
    END_DATE_TIME_KEY  BIGINT,
    TENANT_KEY   BIGINT,
    MEDIA_TYPE_KEY   BIGINT,
    RESOURCE_KEY   BIGINT,
    MEDIA_RESOURCE_KEY   BIGINT,
    PLACE_KEY  BIGINT,
    INTERACTION_RESOURCE_STATE_KEY BIGINT,
    INTERACTION_TYPE_KEY   BIGINT,
    CREATE_AUDIT_KEY   BIGINT,
    UPDATE_AUDIT_KEY   BIGINT,
    INTERACTION_RESOURCE_ID   BIGINT,
    START_TS  BIGINT,
    END_TS  BIGINT,
    TOTAL_DURATION  BIGINT,
    LEAD_CLIP_DURATION  BIGINT,
    TRAIL_CLIP_DURATION   BIGINT,
    TARGET_ADDRESS  VARCHAR(255),
    ACTIVE_FLAG   INT,
    PURGE_FLAG  INT,
    ORIGINAL_FILE_NAME  VARCHAR(50),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE  DATE,
    INSERT_DATE   TIMESTAMP

)
CLUSTERED BY(ORIGINAL_FILE_DATE) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");
