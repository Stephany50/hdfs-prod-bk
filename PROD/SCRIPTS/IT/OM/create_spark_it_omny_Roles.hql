
CREATE TABLE CDR.SPARK_IT_OMNY_ROLES (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
DOMAIN_TYPE              VARCHAR(10),
ROLE_CODE                VARCHAR(20),
ROLE_NAME                VARCHAR(60),
GROUP_NAME               VARCHAR(30),
STATUS                   VARCHAR(5),
GATEWAY_TYPES            VARCHAR(50),
INSERT_DATE          TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(ROLE_CODE) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')