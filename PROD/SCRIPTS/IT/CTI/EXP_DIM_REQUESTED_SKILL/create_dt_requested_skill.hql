CREATE TABLE CTI.DT_REQUESTED_SKILL (
SKILL_KEY              BIGINT,
TENANT_KEY             BIGINT,
SKILL_COMBINATION_KEY  BIGINT,
CREATE_AUDIT_KEY       BIGINT,
UPDATE_AUDIT_KEY       BIGINT,
SKILL_LEVEL            BIGINT,
PURGE_FLAG             INT,
ORIGINAL_FILE_NAME          VARCHAR(50) ,
  ORIGINAL_FILE_SIZE INT,
  ORIGINAL_FILE_LINE_COUNT INT,
  ORIGINAL_FILE_DATE          DATE ,
  INSERT_DATE TIMESTAMP
 )

CLUSTERED BY(ORIGINAL_FILE_DATE) INTO 64 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864")
;