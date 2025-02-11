CREATE TABLE CDR.IT_OMNY_WALLET (
ORIGINAL_FILE_NAME VARCHAR(50),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
WALLET_NUMBER       VARCHAR(60),
MSISDN              VARCHAR(60),
VALID_FROM_DATE     TIMESTAMP,
EXPIRY_DATE         TIMESTAMP,
CREATED_BY          VARCHAR(60),
CREATED_ON          TIMESTAMP,
MODIFIED_ON         TIMESTAMP,
LAST_TRANSFER_ON    TIMESTAMP,
LAST_TRANSFER_TYPE  VARCHAR(60),
ENCRYPTION_DONE     VARCHAR(60),
WALLET_LOCK         VARCHAR(60),
IS_PRIMARY          VARCHAR(60),
PAYMENT_METHOD_TYPE_ID   VARCHAR(60),
USER_WALLET_TYPE         VARCHAR(60),
PROVIDER_ID              INT,
STATUS                   VARCHAR(60),
USER_GRADE               VARCHAR(60),
USER_ID                  VARCHAR(60),
USER_TYPE                VARCHAR(60),
INSERT_DATE             TIMESTAMP

)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(WALLET_NUMBER) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");