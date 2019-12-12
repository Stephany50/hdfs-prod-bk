CREATE TABLE CDR.IT_OMNY_DOMAIN_DETAILS (
ORIGINAL_FILE_NAME        VARCHAR(50),
ORIGINAL_FILE_SIZE        INT,
ORIGINAL_FILE_LINE_COUNT  INT,
DOMAIN_CODE               VARCHAR(10),
DOMAIN_NAME               VARCHAR(50),
DOMAIN_TYPE_CODE          VARCHAR(10),
STATUS                    VARCHAR(10),
OWNER_CATEGORY            VARCHAR(10),
NUM_OF_CATEGORIES         INT,
INSERT_DATE               TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(DOMAIN_CODE) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");