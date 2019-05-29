---***********************************************************---
------------ CTI.DT_GROUP -------------------
----  Arnold Chuenffo 14-05-2019
---***********************************************************---

CREATE TABLE  CTI.DT_GROUP (

GROUP_KEY                INT,       
TENANT_KEY               INT,       
GROUP_NAME               VARCHAR(50), 
CREATE_AUDIT_KEY         INT,       
UPDATE_AUDIT_KEY         INT,       
GROUP_TYPE               VARCHAR(50), 
GROUP_TYPE_CODE          VARCHAR(50), 
GROUP_CFG_DBID           INT,       
GROUP_CFG_TYPE_ID        INT,       
START_TS                 INT,       
END_TS                   INT, 
ORIGINAL_FILE_NAME       VARCHAR(100),
ORIGINAL_FILE_SIZE       INT,
ORIGINAL_FILE_LINE_COUNT INT,
ORIGINAL_FILE_DATE       DATE,
INSERT_DATE	             TIMESTAMP
)
CLUSTERED BY(ORIGINAL_FILE_DATE) INTO 64 BUCKETS
STORED AS ORC 
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;