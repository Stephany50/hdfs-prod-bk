
---***********************************************************---
------------ External Table-TT CTI.DT_USER_DATA_CUST_DIM_5 -----------
---***********************************************************---

CREATE EXTERNAL TABLE IF NOT EXISTS CTI.TT_DT_USER_DATA_CUST_DIM_5(

ID                       INT,
TENANT_KEY               INT,
CREATE_AUDIT_KEY         INT,
DIM_ATTRIBUTE_1          VARCHAR(255),
DIM_ATTRIBUTE_2          VARCHAR(255),
DIM_ATTRIBUTE_3          VARCHAR(255),
DIM_ATTRIBUTE_4          VARCHAR(255),
DIM_ATTRIBUTE_5          VARCHAR(255),
ORIGINAL_FILE_NAME       VARCHAR(200),
ORIGINAL_FILE_SIZE       INT,
ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'CTI.TT_DT_USER_DATA_CUST_DIM_5 external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_USER_DATA_CUST_DIM_5'
TBLPROPERTIES ('serialization.null.format'='')
;
