--***********************************************************---
------------ IT Table- IT_DIMELO_MESSAGES  -------------------
---- NKONGYUM PROSPER AKWO
---***********************************************************---

CREATE TABLE CDR.SPARK_IT_DIMELO_INTERVENTIONS (
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    closed_at TIMESTAMP,
    closed_automatically VARCHAR(1000),
    source_id VARCHAR(1000),
    source_type VARCHAR(1000),
    source_name VARCHAR(1000),
    content_thread_id VARCHAR(1000),
    id VARCHAR(1000),
    status VARCHAR(1000),
    deferred_at VARCHAR(1000),
    user_id VARCHAR(1000),
    user_name VARCHAR(1000),
    user_replies_count VARCHAR(1000),
    first_identity_content_id VARCHAR(1000),
    first_user_reply_id VARCHAR(1000),
    first_user_reply_at TIMESTAMP,
    last_user_reply_at TIMESTAMP,
    last_user_reply_in VARCHAR(1000),
    last_user_reply_in_bh VARCHAR(1000),
    first_user_reply_in VARCHAR(1000),
    first_user_reply_in_bh VARCHAR(1000),
    title VARCHAR(1000),
    identity_id VARCHAR(1000),
    identity_name VARCHAR(1000),
    categories VARCHAR(1000),
    comments_count VARCHAR(1000),
    user_reply_in_average VARCHAR(1000),
    user_reply_in_average_bh VARCHAR(1000),
    user_reply_in_average_count VARCHAR(1000),
    ORIGINAL_FILE_NAME VARCHAR(1000),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;


CREATE EXTERNAL TABLE CDR.TT_DIMELO_INTERVENTIONS (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    created_at VARCHAR(1000),
    updated_at VARCHAR(1000),
    closed_at VARCHAR(1000),
    closed_automatically VARCHAR(1000),
    source_id VARCHAR(1000),
    source_type VARCHAR(1000),
    source_name VARCHAR(1000),
    content_thread_id VARCHAR(1000),
    id VARCHAR(1000),
    status VARCHAR(1000),
    deferred_at VARCHAR(1000),
    user_id VARCHAR(1000),
    user_name VARCHAR(1000),
    user_replies_count VARCHAR(1000),
    first_identity_content_id VARCHAR(1000),
    first_user_reply_id VARCHAR(1000),
    first_user_reply_at VARCHAR(1000),
    last_user_reply_at VARCHAR(1000),
    last_user_reply_in VARCHAR(1000),
    last_user_reply_in_bh VARCHAR(1000),
    first_user_reply_in VARCHAR(1000),
    first_user_reply_in_bh VARCHAR(1000),
    title VARCHAR(1000),
    identity_id VARCHAR(1000),
    identity_name VARCHAR(1000),
    categories VARCHAR(1000),
    comments_count VARCHAR(1000),
    user_reply_in_average VARCHAR(1000),
    user_reply_in_average_bh VARCHAR(1000),
    user_reply_in_average_count VARCHAR(1000)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/DIMELO/INTERVENTIONS'
TBLPROPERTIES ('serialization.null.format'='')
;


CREATE TABLE DEFAULT.SQ_IT_DIMELO_INTERVENTIONS (
    FILE_DATE DATE,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    closed_at TIMESTAMP,
    closed_automatically VARCHAR(1000),
    source_id VARCHAR(1000),
    source_type VARCHAR(1000),
    source_name VARCHAR(1000),
    content_thread_id VARCHAR(1000),
    id VARCHAR(1000),
    status VARCHAR(1000),
    deferred_at VARCHAR(1000),
    user_id VARCHAR(1000),
    user_name VARCHAR(1000),
    user_replies_count VARCHAR(1000),
    first_identity_content_id VARCHAR(1000),
    first_user_reply_id VARCHAR(1000),
    first_user_reply_at TIMESTAMP,
    last_user_reply_at TIMESTAMP,
    last_user_reply_in VARCHAR(1000),
    last_user_reply_in_bh VARCHAR(1000),
    first_user_reply_in VARCHAR(1000),
    first_user_reply_in_bh VARCHAR(1000),
    title VARCHAR(1000),
    identity_id VARCHAR(1000),
    identity_name VARCHAR(1000),
    categories VARCHAR(1000),
    comments_count VARCHAR(1000),
    user_reply_in_average VARCHAR(1000),
    user_reply_in_average_bh VARCHAR(1000),
    user_reply_in_average_count VARCHAR(1000),
    ORIGINAL_FILE_NAME VARCHAR(1000),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')