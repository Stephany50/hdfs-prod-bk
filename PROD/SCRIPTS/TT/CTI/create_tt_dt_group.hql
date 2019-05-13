---***********************************************************---
------------ External Table-TT CTI.TT_DT_GROUP -----------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_GROUP (
GROUP_KEY                VARCHAR(32),       
TENANT_KEY               VARCHAR(32),       
GROUP_NAME               VARCHAR(50), 
CREATE_AUDIT_KEY         VARCHAR(32),       
UPDATE_AUDIT_KEY         VARCHAR(32),       
GROUP_TYPE               VARCHAR(50), 
GROUP_TYPE_CODE          VARCHAR(50), 
GROUP_CFG_DBID           VARCHAR(32),       
GROUP_CFG_TYPE_ID        VARCHAR(32),       
START_TS                 VARCHAR(32),       
END_TS                   VARCHAR(32),  
ORIGINAL_FILE_NAME       VARCHAR(200) ,
ORIGINAL_FILE_SIZE               INT,
ORIGINAL_FILE_LINE_COUNT         INT
)
COMMENT 'TT_DT_GROUP external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_GROUP_/'
TBLPROPERTIES ('serialization.null.format'='')

