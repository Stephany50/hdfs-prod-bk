CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_ADMIN_AUDIT_TRAIL (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
USER_ID                 VARCHAR(30),
USER_MSISDN             VARCHAR(30),
LOGGED_IN               VARCHAR(30),
LOG_OUT                 VARCHAR(30),
CATEGORY                VARCHAR(30),
ACTION_TYPE             VARCHAR(255),
ACTION_PERFORMED_ON     VARCHAR(60),
BARRED_USER             VARCHAR(60),
REMARKS                 VARCHAR(255),
CREATED_BY              VARCHAR(60),
CREATED_ON              VARCHAR(30),
ATT_1_NAME              VARCHAR(255),
ATT_1_VALUE             VARCHAR(255),
ATT_2_NAME              VARCHAR(255),
ATT_2_VALUE             VARCHAR(255),
ATT_3_NAME              VARCHAR(255),
ATT_3_VALUE             VARCHAR(255),
SN                      VARCHAR(255),
TRANSACTION_ID          VARCHAR(255),
CUST_ID                 VARCHAR(255),
STATUS_ID               VARCHAR(255)
)
COMMENT 'OM AdminAuditTrail'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/ADMINAUDITTRAIL/'
TBLPROPERTIES ('serialization.null.format'='');