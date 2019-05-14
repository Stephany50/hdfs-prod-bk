CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_EXP_FACT_SM_RES_STATE_REASON_FACT (
    SM_RES_STATE_REASON_FACT_KEY VARCHAR(50),
    TENANT_KEY VARCHAR(50),
    CREATE_AUDIT_KEY VARCHAR(50),
    UPDATE_AUDIT_KEY VARCHAR(50),
    START_DATE_TIME_KEY VARCHAR(50),
    END_DATE_TIME_KEY VARCHAR(50),
    RESOURCE_STATE_KEY VARCHAR(50),
    RESOURCE_STATE_REASON_KEY VARCHAR(50),
    MEDIA_TYPE_KEY VARCHAR(50),
    RESOURCE_KEY VARCHAR(50),
    RESOURCE_GROUP_COMBINATION_KEY VARCHAR(50),
    SM_RES_SESSION_FACT_KEY VARCHAR(50),
    SM_RES_STATE_FACT_KEY VARCHAR(50),
    START_TS VARCHAR(50),
    END_TS VARCHAR(50),
    TOTAL_DURATION VARCHAR(50),
    LEAD_CLIP_DURATION VARCHAR(50),
    TRAIL_CLIP_DURATION VARCHAR(50),
    ACTIVE_FLAG VARCHAR(50),
    PURGE_FLAG VARCHAR(50),
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'CTI EXP_FACT_SM_RES_STATE_REASON_FACT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/RAW/CTI/EXP_FACT_SM_RES_STATE_REASON_FACT'
TBLPROPERTIES ('serialization.null.format'='');
