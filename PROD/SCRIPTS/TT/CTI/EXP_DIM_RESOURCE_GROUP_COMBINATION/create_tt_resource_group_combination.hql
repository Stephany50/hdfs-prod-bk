CREATE EXTERNAL TABLE IF NOT EXISTS CTI.tt_resource_group_combination (

GROUP_COMBINATION_KEY    VARCHAR(10)   ,
GROUP_KEY                VARCHAR(10) ,
TENANT_KEY               VARCHAR(10) ,
CREATE_AUDIT_KEY         VARCHAR(19) ,
UPDATE_AUDIT_KEY         VARCHAR(19)  ,
ORIGINAL_FILE_NAME             VARCHAR(50),
ORIGINAL_FILE_SIZE    INT,
ORIGINAL_FILE_LINE_COUNT INT
)
COMMENT 'CTI external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/CTI/EXP_DIM_RESOURCE_GROUP_COMBINATION'
TBLPROPERTIES ('serialization.null.format'='')
;

