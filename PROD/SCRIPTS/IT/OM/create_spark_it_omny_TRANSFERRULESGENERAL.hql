
CREATE TABLE CDR.SPARK_IT_OMNY_TRANSFERRULESGENERAL (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
TRANSFER_RULE_ID        INT,
SERVICE_NAME            VARCHAR(50),
SERVICE_TYPE            VARCHAR(10),
PAYER_DOMAIN_CODE       VARCHAR(10),
PAYEE_DOMAIN_CODE       VARCHAR(10),
PAYER_CATEGORY_CODE     VARCHAR(10),
PAYEE_CATEGORY_CODE     VARCHAR(10),
PAYMENT_METHOD_TYPE     VARCHAR(20),
STATUS_ID               VARCHAR(5),
TRANSFER_TYPE           VARCHAR(10),
CREATED_BY              VARCHAR(20),
GRPH_DOMAIN_CODE        VARCHAR(10),
PAYER_PAYMENT_TYPE_ID   INT,
PAYEE_PAYMENT_TYPE_ID   INT,
CREATED_ON              TIMESTAMP,
MODIFIED_BY             VARCHAR(30),
BYPASS_ALLOWED          VARCHAR(5),
DIRECT_TRANSFER_ALLOWED  VARCHAR(5),
INSERT_DATE          TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(TRANSFER_RULE_ID) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')