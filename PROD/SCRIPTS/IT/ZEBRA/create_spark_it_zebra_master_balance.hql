CREATE TABLE cdr.spark_it_zebra_master_balance (
    EVENT_TIME                 VARCHAR(2),
    CHANNEL_USER_ID            VARCHAR(50),
    USER_NAME                  VARCHAR(200),
    MOBILE_NUMBER              VARCHAR(50),
    CATEGORY                   VARCHAR(50),
    MOBILE_NUMBER_1            VARCHAR(50),
    GEOGRAPHICAL_DOMAIN        VARCHAR(50),
    PRODUCT                    VARCHAR(50),
    PARENT_USER_NAME           VARCHAR(200),
    OWNER_USER_NAME            VARCHAR(200),
    AVAILABLE_BALANCE          BIGINT,
    AGENT_BALANCE              BIGINT,
    ORIGINAL_FILE_NAME         VARCHAR(50),
    ORIGINAL_FILE_DATE         DATE,
    INSERT_DATE                TIMESTAMP,
    USER_STATUS                VARCHAR(200),
    TO_CHANGE                  VARCHAR(500),
    MODIFIED_ON                DATE,
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT
)
    COMMENT 'Subs Retail Zebra - FT'
    PARTITIONED BY (EVENT_DATE DATE)
    STORED AS PARQUET
    TBLPROPERTIES ("parquet.compress"="SNAPPY")


insert into  cdr.spark_it_zebra_master_balance select
EVENT_TIME,
CHANNEL_USER_ID,
USER_NAME,
MOBILE_NUMBER,
CATEGORY,
MOBILE_NUMBER_1,
GEOGRAPHICAL_DOMAIN,
PRODUCT,
PARENT_USER_NAME,
OWNER_USER_NAME,
AVAILABLE_BALANCE,
AGENT_BALANCE,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_DATE,
INSERT_DATE,
USER_STATUS,
TO_CHANGE,
MODIFIED_ON,
null ORIGINAL_FILE_SIZE,
null ORIGINAL_FILE_LINE_COUNT,
to_date(EVENT_DATE)
from backup_dwh.it_zebra_master_balance5