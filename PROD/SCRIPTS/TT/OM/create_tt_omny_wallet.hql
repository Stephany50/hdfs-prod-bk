CREATE EXTERNAL TABLE IF NOT EXISTS CDR.TT_OMNY_WALLET (
ORIGINAL_FILE_NAME VARCHAR(50),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
WALLET_NUMBER       VARCHAR(60),
MSISDN              VARCHAR(60),
VALID_FROM_DATE     VARCHAR(60),
EXPIRY_DATE         VARCHAR(60),
CREATED_BY          VARCHAR(60),
CREATED_ON          VARCHAR(60),
MODIFIED_ON         VARCHAR(60),
LAST_TRANSFER_ON    VARCHAR(60),
LAST_TRANSFER_TYPE  VARCHAR(60),
ENCRYPTION_DONE     VARCHAR(60),
WALLET_LOCK         VARCHAR(60),
IS_PRIMARY          VARCHAR(60),
PAYMENT_METHOD_TYPE_ID   VARCHAR(60),
USER_WALLET_TYPE         VARCHAR(60),
PROVIDER_ID              VARCHAR(60),
STATUS                   VARCHAR(60),
USER_GRADE               VARCHAR(60),
USER_ID                  VARCHAR(60),
USER_TYPE                VARCHAR(60)


)
COMMENT 'OM WALLET'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/OM/WALLET'
TBLPROPERTIES ('serialization.null.format'='');