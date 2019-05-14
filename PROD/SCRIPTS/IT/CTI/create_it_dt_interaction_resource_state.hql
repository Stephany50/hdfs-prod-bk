---***********************************************************---
------------ CTI.IT_DT_INTERACTION_RESOURCE_STATE -----------
----  Arnold Chuenffo 14-05-2019
---***********************************************************---

CREATE TABLE  CTI.IT_DT_INTERACTION_RESOURCE_STATE (  

INTERACTION_RESOURCE_STATE_KEY INT,   
CREATE_AUDIT_KEY               BIGINT,   
UPDATE_AUDIT_KEY               BIGINT,   
STATE_NAME                     VARCHAR(64), 
STATE_NAME_CODE                VARCHAR(32), 
STATE_ROLE                     VARCHAR(64), 
STATE_ROLE_CODE                VARCHAR(32), 
STATE_DESCRIPTOR               VARCHAR(64), 
STATE_DESCRIPTOR_CODE          VARCHAR(32), 
PURGE_FLAG                     INT, 
ORIGINAL_FILE_NAME         VARCHAR(100),
ORIGINAL_FILE_SIZE         INT,
ORIGINAL_FILE_LINE_COUNT   INT,
ORIGINAL_FILE_DATE         DATE,
INSERT_DATE	              TIMESTAMP
)
CLUSTERED BY(ORIGINAL_FILE_DATE) INTO 64 BUCKETS
STORED AS ORC 
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;