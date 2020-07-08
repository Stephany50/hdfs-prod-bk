CREATE TABLE CDR.SPARK_IT_OMNY_BARRED_ACCOUNTS (
ORIGINAL_FILE_NAME     VARCHAR(50),
ORIGINAL_FILE_SIZE     INT,
ORIGINAL_FILE_LINE_COUNT INT,
ACTION                  VARCHAR(10),
SYS_TIMESTAMP          TIMESTAMP,
MSISDN     VARCHAR(10),
PARTY_USER_ID        VARCHAR(50),
USER_NAME          VARCHAR(180),
PARENT_NAME        VARCHAR(10),
PARTY_LOGIN_ID           VARCHAR(30),
TYPE                   VARCHAR(30),
BARRING_REASON               VARCHAR(30),
ACTION_PERFORMED_BY               VARCHAR(30)
INSERT_DATE               TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(BARRING_TYPE) INTO 8 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
