CREATE EXTERNAL TABLE IF NOT EXISTS CTI.tt_exp_dim_resource (
RESOURCE_KEY           VARCHAR(10),
TENANT_KEY             VARCHAR(10),
CREATE_AUDIT_KEY       VARCHAR(19),
UPDATE_AUDIT_KEY       VARCHAR(19),
SWITCH_NAME                    VARCHAR(255),
IVR_NAME                       VARCHAR(255),
RESOURCE_TYPE                  VARCHAR(255),
RESOURCE_TYPE_CODE             VARCHAR(32),
RESOURCE_SUBTYPE               VARCHAR(255),
RESOURCE_NAME                  VARCHAR(255),
AGENT_FIRST_NAME               VARCHAR(64),
AGENT_LAST_NAME                VARCHAR(64),
EMPLOYEE_ID                    VARCHAR(255),
EXTERNAL_RESOURCE_ID           VARCHAR(255),
RESOURCE_CFG_DBID              VARCHAR(10),
RESOURCE_CFG_TYPE_ID           VARCHAR(10),
RESOURCE_ALIAS                 VARCHAR(255),
NETWORK_RESOURCE_FLAG          VARCHAR(1) ,
GMT_START_TIME                 TIMESTAMP,
GMT_END_TIME                   TIMESTAMP,
PURGE_FLAG                     VARCHAR(1),
ORIGINAL_FILE_NAME             VARCHAR(50),
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'CTI external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_RESOURCE'
TBLPROPERTIES ('serialization.null.format'='')
;
