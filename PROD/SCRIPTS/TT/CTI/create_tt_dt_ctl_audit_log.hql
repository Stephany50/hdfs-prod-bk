---***********************************************************---
------------ External Table-TT DT_CTL_AUDIT_LOG -----------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_CTL_AUDIT_LOG (

AUDIT_KEY                        VARCHAR(100) ,  
JOB_ID                           VARCHAR(100) ,
CREATED                          VARCHAR(100) ,
INSERTED                         VARCHAR(100) ,
PROCESSING_STATUS_KEY            VARCHAR(100) ,
MIN_START_DATE_TIME_KEY          VARCHAR(100) ,
MAX_START_DATE_TIME_KEY          VARCHAR(100) ,
MAX_CHUNK_TS                     VARCHAR(100) ,
DATA_SOURCE_KEY                  VARCHAR(100) ,
ROW_COUNT                        VARCHAR(100) ,
ORIGINAL_FILE_NAME               VARCHAR(200) ,
ORIGINAL_FILE_SIZE               INT,
ORIGINAL_FILE_LINE_COUNT         INT
)
COMMENT 'EXP_DIM_CTL_AUDIT_LOG external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_CTL_AUDIT_LOG/'
TBLPROPERTIES ('serialization.null.format'='')