
CREATE  TABLE IF NOT EXISTS CTI.FT_IRF_USER_DATA_CUST_1 (
    INTERACTION_RESOURCE_ID   BIGINT,
    START_DATE_TIME_KEY   BIGINT,
    TENANT_KEY  BIGINT,
    CUSTOM_DATA_1   VARCHAR(255),
    CUSTOM_DATA_2   VARCHAR(255),
    CUSTOM_DATA_3   VARCHAR(255),
    CUSTOM_DATA_4   VARCHAR(255),
    CUSTOM_DATA_5   VARCHAR(255),
    CUSTOM_DATA_6   VARCHAR(255),
    CUSTOM_DATA_7   VARCHAR(255),
    CUSTOM_DATA_8   VARCHAR(255),
    CUSTOM_DATA_9   VARCHAR(255),
    CUSTOM_DATA_10  VARCHAR(255),
    CUSTOM_DATA_11  VARCHAR(255),
    CUSTOM_DATA_12  VARCHAR(255),
    CUSTOM_DATA_13  VARCHAR(255),
    CUSTOM_DATA_14  VARCHAR(255),
    CUSTOM_DATA_15  VARCHAR(255),
    CUSTOM_DATA_16  VARCHAR(255),
    ORIGINAL_FILE_NAME  VARCHAR(50),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE  DATE,
    INSERT_DATE   TIMESTAMP
)
CLUSTERED BY(ORIGINAL_FILE_DATE) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");