CREATE TABLE CDR.IT_OMNY_PIN_RESET (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
TARGET_USER_ID       VARCHAR(20),
TARGET_USER_MSISDN   VARCHAR(15),
TARGET_USER_CATEGORY_CODE    VARCHAR(10),
TARGET_USER_NAME             VARCHAR(80),
TARGET_USER_LAST_NAME        VARCHAR(80),
CHANGED_BY_USER_ID            VARCHAR(20),
CHANGED_BY_USER_MSISDN        VARCHAR(15),
CHANGED_BY_USER_CATEGORY_CODE   VARCHAR(10),
CHANGED_BY_USER_LOGIN_ID         VARCHAR(20),
CHANGED_BY_USER_NAME            VARCHAR(80),
CHANGED_BY_USER_LAST_NAME        VARCHAR(80),
CREATED_ON                       TIMESTAMP,
ACTION_TYPE                      VARCHAR(20),
REMARKS                          VARCHAR(10),
INSERT_DATE               TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(TARGET_USER_ID) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");