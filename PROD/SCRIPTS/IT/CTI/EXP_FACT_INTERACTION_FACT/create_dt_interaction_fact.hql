CREATE TABLE CTI.FT_INTERACTION_FACT(

INTERACTION_ID                   BIGINT,
TENANT_KEY                       INT,
INTERACTION_TYPE_KEY             INT,
MEDIA_TYPE_KEY                   INT,
MEDIA_SERVER_ROOT_IXN_ID         BIGINT,
MEDIA_SERVER_IXN_ID              BIGINT,
MEDIA_SERVER_ROOT_IXN_GUID       VARCHAR(50),
MEDIA_SERVER_IXN_GUID            VARCHAR(50),
SOURCE_ADDRESS                   VARCHAR(100),
TARGET_ADDRESS                   VARCHAR(100),
SUBJECT                          VARCHAR(100),
STATUS                           INT,
START_TS                         INT,
END_TS                           INT,
START_DATE_TIME_KEY              INT,
END_DATE_TIME_KEY                INT,
CREATE_AUDIT_KEY                 BIGINT,
UPDATE_AUDIT_KEY                 BIGINT,
ACTIVE_FLAG                      INT,
PURGE_FLAG                       INT,
ORIGINAL_FILE_NAME               VARCHAR(50),
ORIGINAL_FILE_DATE               DATE,
ORIGINAL_FILE_SIZE               INT,
ORIGINAL_FILE_LINE_COUNT         INT,
INSERT_DATE                      TIMESTAMP

)
CLUSTERED BY(ORIGINAL_FILE_DATE) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;