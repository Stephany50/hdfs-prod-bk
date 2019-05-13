---***********************************************************---
------------ External Table-TT CTI.DT_INTERACTION_RESOURCE_STATE ------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_INTERACTION_RESOURCE_STATE (
INTERACTION_RESOURCE_STATE_KEY VARCHAR(32),    
CREATE_AUDIT_KEY               VARCHAR(32),   
UPDATE_AUDIT_KEY               VARCHAR(32),   
STATE_NAME                     VARCHAR(64), 
STATE_NAME_CODE                VARCHAR(32), 
STATE_ROLE                     VARCHAR(64), 
STATE_ROLE_CODE                VARCHAR(32), 
STATE_DESCRIPTOR               VARCHAR(64), 
STATE_DESCRIPTOR_CODE          VARCHAR(32), 
DUMMY_COLUMN                   VARCHAR(32), 
ORIGINAL_FILE_NAME             VARCHAR(200),
ORIGINAL_FILE_SIZE               INT,
ORIGINAL_FILE_LINE_COUNT         INT
)
COMMENT 'DT_INTERACTION_RESOURCE_STATE external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_INTERACTION_RESOURCE_STATE/'
TBLPROPERTIES ('serialization.null.format'='')
