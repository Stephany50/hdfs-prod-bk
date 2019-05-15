
---***********************************************************---
------------ External Table-TT CTI.DT_USER_DATA_CUST_DIM_4 -----------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_USER_DATA_CUST_DIM_4(


ID                       INT,
TENANT_KEY               INT,
CREATE_AUDIT_KEY         INT,
DIM_ATTRIBUTE_1          VARCHAR(255) ,
DIM_ATTRIBUTE_2          VARCHAR(255) ,
DIM_ATTRIBUTE_3          VARCHAR(255) ,
DIM_ATTRIBUTE_4          VARCHAR(255) ,
DIM_ATTRIBUTE_5          VARCHAR(255) ,
ORIGINAL_FILE_NAME       VARCHAR(255) ,
ORIGINAL_FILE_SIZE       INT,
ORIGINAL_FILE_LINE_COUNT INT


)
COMMENT 'CTI.DT_USER_DATA_CUST_DIM_4 external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_USER_DATA_CUST_DIM_4'
TBLPROPERTIES ('serialization.null.format'='')
;