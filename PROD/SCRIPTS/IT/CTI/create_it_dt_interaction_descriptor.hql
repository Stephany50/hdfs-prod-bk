---***********************************************************---
------------ CTI.TT_INTERACTION_DESCRIPTOR -------------------
----  Arnold Chuenffo 14-05-2019
---***********************************************************---

CREATE TABLE  CTI.IT_INTERACTION_DESCRIPTOR (  

INTERACTION_DESCRIPTOR_KEY INT,    
TENANT_KEY                 INT,    
CREATE_AUDIT_KEY           BIGINT,    
CUSTOMER_SEGMENT           VARCHAR(255), 
SERVICE_TYPE               VARCHAR(255), 
SERVICE_SUBTYPE            VARCHAR(255), 
BUSINESS_RESULT            VARCHAR(255), 
PURGE_FLAG                 INT,
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