CREATE TABLE CDR.SPARK_IT_DIMELO_SOURCES (
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    id varchar(10000),
    active varchar(10000),
    name varchar(10000),
    status varchar(10000),
    community_type varchar(10000),
    community varchar(10000),
    content_archiving varchar(10000),
    auto_detect_content_language varchar(10000),
    content_languages varchar(10000),
    default_content_language varchar(10000),
    content_signature varchar(10000),
    hidden_from_stats varchar(10000),
    default_task_priority varchar(10000),
    channel_id varchar(10000),
    channel_name varchar(10000),
    sla_expired_strategy varchar(10000),
    sla_response varchar(10000),
    intervention_messages_boost varchar(10000),
    private_messages_supported varchar(10000),
    ORIGINAL_FILE_NAME VARCHAR(1000),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;



CREATE EXTERNAL TABLE CDR.SPARK_TT_DIMELO_SOURCES (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    created_at varchar(10000),
    updated_at varchar(10000),
    id varchar(10000),
    active varchar(10000),
    name varchar(10000),
    status varchar(10000),
    community_type varchar(10000),
    community varchar(10000),
    content_archiving varchar(10000),
    auto_detect_content_language varchar(10000),
    content_languages varchar(10000),
    default_content_language varchar(10000),
    content_signature varchar(10000),
    hidden_from_stats varchar(10000),
    default_task_priority varchar(10000),
    channel_id varchar(10000),
    channel_name varchar(10000),
    sla_expired_strategy varchar(10000),
    sla_response varchar(10000),
    intervention_messages_boost varchar(10000),
    private_messages_supported varchar(10000)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/DIMELO/SOURCES'
TBLPROPERTIES ('serialization.null.format'='');