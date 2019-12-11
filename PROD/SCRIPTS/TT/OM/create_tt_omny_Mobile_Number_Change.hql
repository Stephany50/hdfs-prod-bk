CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_MOBILE_NUMBER_CHANGE (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
USER_ID                VARCHAR(20),
USER_NAME              VARCHAR(160),
OLD_ACCOUNT_NUMBER     VARCHAR(20),
NEW_ACCOUNT_NUMBER     VARCHAR(20),
MODIFIED_BY_USER_ID    VARCHAR(20),
MODIFIED_BY_USER_NAME  VARCHAR(180),
MODIFIED_ON            VARCHAR(20),
DETAIL_MODIFICATION    VARCHAR(20),
)
COMMENT 'OM MOBILE_NUMBER_CHANGE'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/MOBILE_NUMBER_CHANGE'
TBLPROPERTIES ('serialization.null.format'='');
