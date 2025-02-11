CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_EXP_GLOB_GIDB_G_CALL_V (
    ID VARCHAR(50),
    CALLID VARCHAR(50),
    PARENTCALLID VARCHAR(50),
    MERGECALLID VARCHAR(50),
    MERGETYPE VARCHAR(50),
    CONNID VARCHAR(50),
    CONNIDNUM VARCHAR(50),
    SWITCHCALLID VARCHAR(50),
    IRID VARCHAR(50),
    ROOTIRID VARCHAR(50),
    STATE VARCHAR(50),
    CALLTYPE VARCHAR(50),
    MEDIATYPE VARCHAR(50),
    SWITCHID VARCHAR(50),
    TENANTID VARCHAR(50),
    CALLANI VARCHAR(50),
    CALLDNIS VARCHAR(50),
    CREATED VARCHAR(50),
    CREATED_TS VARCHAR(50),
    TERMINATED VARCHAR(50),
    TERMINATED_TS VARCHAR(50),
    GSYS_DOMAIN VARCHAR(50),
    GSYS_SYS_ID VARCHAR(50),
    GSYS_EXT_VCH1 VARCHAR(50),
    GSYS_EXT_VCH2 VARCHAR(50),
    GSYS_EXT_INT1 VARCHAR(50),
    GSYS_EXT_INT2 VARCHAR(50),
    CREATE_AUDIT_KEY VARCHAR(50),
    UPDATE_AUDIT_KEY VARCHAR(50),
    ORIGINAL_FILE_NAME VARCHAR(100),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'CTI EXP_GLOB_GIDB_G_CALL_V'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_GLOB_GIDB_G_CALL_V'
TBLPROPERTIES ('serialization.null.format'='');
