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
    
    

insert into cdr.spark_it_zebra_master_balance
    select
    event_time,
    channel_user_id,
    user_name,
    mobile_number,
    category,
    mobile_number_1,
    geographical_domain,
    product,
    parent_user_name,
    owner_user_name,
    available_balance,
    agent_balance,
    original_file_name,
    to_date(original_file_date) original_file_date,
    insert_date,
    user_status,
    to_change,
    to_date(modified_on) modified_on,
    NULL original_file_size,
    NULL original_file_line_count,
    to_date(event_date) event_date
FROM backup_dwh.it_zebra_master_balance9
