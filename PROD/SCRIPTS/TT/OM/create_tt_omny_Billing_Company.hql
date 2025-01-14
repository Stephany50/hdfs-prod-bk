CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_BILLING_COMPANY (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
COMPANY_CATEGORY        VARCHAR(80),
COMPANY_NAME            VARCHAR(80),
COMPANY_CODE            VARCHAR(80),
ADDRESS                 VARCHAR(80),
CITY                    VARCHAR(80),
DISTRICT                VARCHAR(80),
COUNTRY                 VARCHAR(80),
ACCOUNT_STATUS           VARCHAR(80),
CREATED_ON               VARCHAR(80),
NOTIFICATION_NAME        VARCHAR(80),
DELETION_APPROVED_ON     VARCHAR(80),
DELETION_APPROVED_BY     VARCHAR(80),
LOGIN_ID                 VARCHAR(80),
EMAIL                    VARCHAR(80),
CONTACT_PERSON           VARCHAR(80),
CONTACT_NUMBER           VARCHAR(80),
PAID_BILL_NOTIFICATION      VARCHAR(80),
AUTO_BILL_DELETE_FREQUENCY  VARCHAR(80),
USER_PROFILE_ID             VARCHAR(80),
PROFILE_NAME                VARCHAR(80),
CREATION_INITIATED_ON       VARCHAR(80),
CREATION_INITIATED_BY        VARCHAR(80),
CREATION_APPROVED_ON         VARCHAR(80),
CREATION_APPROVED_BY         VARCHAR(80),
LAST_MODIFIED_ON              VARCHAR(80),
LAST_MODIFIED_BY              VARCHAR(80),
DELETION_INITIATED_ON          VARCHAR(80),
DELETION_INITIATED_BY          VARCHAR(80),
)
COMMENT 'OM BILLING_COMPANY'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/BILLING_COMPANY'
TBLPROPERTIES ('serialization.null.format'='');