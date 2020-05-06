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
