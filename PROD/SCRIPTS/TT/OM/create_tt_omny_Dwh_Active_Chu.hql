
CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_DWH_ACTIVE_CHU (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
MSISDN                    VARCHAR(15),
USER_ID                   VARCHAR(20),
USER_NAME                 VARCHAR(80),
LAST_NAME                 VARCHAR(80),
CATEGORY_CODE             VARCHAR(10),
TRANSACTION_TYPE          VARCHAR(6),
SERVICE_TYPE              VARCHAR(20),
LAST_TRANSFER_DATE        VARCHAR(15)
)
COMMENT 'OM DWH_ACTIVE_CHU'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/DWH_ACTIVE_CHU'
TBLPROPERTIES ('serialization.null.format'='');