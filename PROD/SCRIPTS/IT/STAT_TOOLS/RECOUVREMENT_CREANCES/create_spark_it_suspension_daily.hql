CREATE TABLE CDR.SPARK_IT_SUSPENSION_DAILY (
ORIGINAL_FILE_NAME VARCHAR(150),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
ACCOUNT_NUMBER   VARCHAR(100),
ACCOUNT_NAME     VARCHAR(255),
SERVICE_NUMBER   VARCHAR(100),
PROFILE          VARCHAR(255),
SUSPENSION_REASON VARCHAR(255),
SUSPENSION_TIME   VARCHAR(100),
STAFF_NAME        VARCHAR(100),
INSERT_DATE TIMESTAMP,
ORIGINAL_FILE_DATE    DATE
)
PARTITIONED BY (DATE_SUSPENSION DATE)
STORED AS PARQUET TBLPROPERTIES ("parquet.compress"="SNAPPY")