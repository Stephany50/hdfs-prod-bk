---***********************************************************---
------------ CTI.DT_INTERACTION_TYPE -------------------
----  Arnold Chuenffo 14-05-2019
---***********************************************************---

CREATE TABLE  CTI.DT_INTERACTION_TYPE (

INTERACTION_TYPE_KEY       INT,   
INTERACTION_TYPE           VARCHAR(64), 
INTERACTION_TYPE_CODE      VARCHAR(32), 
INTERACTION_SUBTYPE        VARCHAR(64), 
INTERACTION_SUBTYPE_CODE   VARCHAR(32), 
IGNORE                     INT,    
CREATE_AUDIT_KEY           BIGINT,   
UPDATE_AUDIT_KEY           BIGINT, 
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