
---***********************************************************---
------------ External Table-CTI.TT_INTERACTION_FACT -----------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_INTERACTION_FACT (

INTERACTION_ID                   VARCHAR(32),
TENANT_KEY                       VARCHAR(32),
INTERACTION_TYPE_KEY             VARCHAR(32),
MEDIA_TYPE_KEY                   VARCHAR(32),
MEDIA_SERVER_ROOT_IXN_ID         VARCHAR(32),
MEDIA_SERVER_IXN_ID              VARCHAR(32),
MEDIA_SERVER_ROOT_IXN_GUID       VARCHAR(100),
MEDIA_SERVER_IXN_GUID            VARCHAR(100),
SOURCE_ADDRESS                   VARCHAR(255),
TARGET_ADDRESS                   VARCHAR(255),
SUBJECT                          VARCHAR(255),
STATUS                           VARCHAR(32),
START_TS                         VARCHAR(32),
END_TS                           VARCHAR(32),
START_DATE_TIME_KEY              VARCHAR(32),
END_DATE_TIME_KEY                VARCHAR(32),
CREATE_AUDIT_KEY                 VARCHAR(32),
UPDATE_AUDIT_KEY                 VARCHAR(32),
ACTIVE_FLAG                      VARCHAR(32),
PURGE_FLAG                       VARCHAR(32),
ORIGINAL_FILE_NAME               VARCHAR(200) ,
ORIGINAL_FILE_SIZE               INT,
ORIGINAL_FILE_LINE_COUNT         INT

)
COMMENT 'CTI.TT_INTERACTION_FACT external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_FACT_INTERACTION_FACT'
TBLPROPERTIES ('serialization.null.format'='')
;