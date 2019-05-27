---***********************************************************---
------------ External Table-TT TT_INTERACTION_DESCRIPTOR ------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_INTERACTION_DESCRIPTOR (
INTERACTION_DESCRIPTOR_KEY VARCHAR(32),    
TENANT_KEY                 VARCHAR(32),    
CREATE_AUDIT_KEY           VARCHAR(32),    
CUSTOMER_SEGMENT           VARCHAR(255), 
SERVICE_TYPE               VARCHAR(255), 
SERVICE_SUBTYPE            VARCHAR(255), 
BUSINESS_RESULT            VARCHAR(255), 
PURGE_FLAG                 VARCHAR(255),  
ORIGINAL_FILE_NAME         VARCHAR(200) ,
ORIGINAL_FILE_SIZE               INT,
ORIGINAL_FILE_LINE_COUNT         INT
)
COMMENT 'CTI.DT_INTERACTION_DESCRIPTOR external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_INTERACTION_DESCRIPTOR/'
TBLPROPERTIES ('serialization.null.format'='')

