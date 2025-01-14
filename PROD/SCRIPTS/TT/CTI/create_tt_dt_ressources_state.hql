
CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_RESOURCE_STATE (
    RESOURCE_STATE_KEY   VARCHAR(10)   ,
    STATE_TYPE                  VARCHAR(64) ,
    STATE_TYPE_CODE             VARCHAR(32) ,
    STATE_NAME                  VARCHAR(64) ,
    STATE_NAME_CODE             VARCHAR(32) ,
    CREATE_AUDIT_KEY     VARCHAR(19)   ,
    UPDATE_AUDIT_KEY     VARCHAR(19)   ,
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u003B'
LOCATION '/PROD/TT/CTI/EXP_DIM_RESOURCE_STATE'
TBLPROPERTIES ('serialization.null.format'='')