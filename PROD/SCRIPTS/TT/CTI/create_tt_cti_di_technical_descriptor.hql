
CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_TECHNICAL_DESCRIPTOR (
    TECHNICAL_DESCRIPTOR_KEY   VARCHAR(10),
    TECHNICAL_RESULT                  VARCHAR(255),
    TECHNICAL_RESULT_CODE             VARCHAR(32),
    RESULT_REASON                     VARCHAR(255),
    RESULT_REASON_CODE                VARCHAR(32),
    RESOURCE_ROLE                     VARCHAR(255),
    RESOURCE_ROLE_CODE                VARCHAR(32),
    ROLE_REASON                       VARCHAR(255),
    ROLE_REASON_CODE                  VARCHAR(32),
    CREATE_AUDIT_KEY           VARCHAR(19),
    UPDATE_AUDIT_KEY           VARCHAR(19),
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\u003B'
LOCATION '/PROD/TT/CTI/EXP_DIM_TECHNICAL_DESCRIPTOR'
TBLPROPERTIES ('serialization.null.format'='')
