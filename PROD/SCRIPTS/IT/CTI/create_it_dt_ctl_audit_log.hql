---***********************************************************---
------------ IT_DT_CTL_AUDIT_LOG -------------------
----  Arnold Chuenffo 14-05-2019
---***********************************************************---

CREATE TABLE  CTI.IT_DT_CTL_AUDIT_LOG (    
AUDIT_KEY                BIGINT,   
JOB_ID                   VARCHAR(64), 
CREATED                  TIMESTAMP,
INSERTED                 TIMESTAMP,
PROCESSING_STATUS_KEY    INT,   
MIN_START_DATE_TIME_KEY  INT,   
MAX_START_DATE_TIME_KEY  INT,   
MAX_CHUNK_TS             INT,  
DATA_SOURCE_KEY          INT,   
ROW_COUNT                INT,  	
ORIGINAL_FILE_NAME       VARCHAR(100) ,
ORIGINAL_FILE_SIZE       INT,
ORIGINAL_FILE_LINE_COUNT INT,
ORIGINAL_FILE_DATE       DATE,
INSERT_DATE	             TIMESTAMP
)
CLUSTERED BY(ORIGINAL_FILE_DATE) INTO 64 BUCKETS
STORED AS ORC 
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;