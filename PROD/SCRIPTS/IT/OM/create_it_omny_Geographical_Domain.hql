
CREATE TABLE CDR.IT_OMNY_GEOGRAPHICAL_DOMAIN (
ORIGINAL_FILE_NAME        VARCHAR(50),
ORIGINAL_FILE_SIZE        INT,
ORIGINAL_FILE_LINE_COUNT  INT,
GRPH_DOMAIN_CODE          VARCHAR(10),
NETWORK_NAME              VARCHAR(50),
PARENT_GRPH_DOMAIN_NAME   VARCHAR(50),
PARENT_GRPH_DOMAIN_TYPE    VARCHAR(10),
GRPH_DOMAIN_TYPE            VARCHAR(10),
GRPH_DOMAIN_SHORT_NAME       VARCHAR(50),
DESCRIPTION                  VARCHAR(50),
STATUS                        VARCHAR(5),
INSERT_DATE                    TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(GRPH_DOMAIN_CODE) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true',"orc.compress"="ZLIB","orc.stripe.size"="67108864");