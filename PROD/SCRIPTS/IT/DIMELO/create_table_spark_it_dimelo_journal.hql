CREATE TABLE CDR.SPARK_IT_DIMELO_JOURNAL (
    id varchar(10000),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    user_id varchar(10000),
    user_name varchar(10000),
    name varchar(10000),
    message varchar(10000),
    content_thread_id varchar(10000),
    content_source_id varchar(10000),
    content_id varchar(10000),
    category_ids varchar(10000),
    task_id varchar(10000),
    action varchar(10000),
    step varchar(10000),
    initial_category_ids varchar(10000),
    ORIGINAL_FILE_NAME VARCHAR(1000),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    ORIGINAL_FILE_DATE DATE,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
;


CREATE EXTERNAL TABLE CDR.SPARK_TT_DIMELO_JOURNAL (
    ORIGINAL_FILE_NAME VARCHAR(200),
    ORIGINAL_FILE_SIZE INT,
    ORIGINAL_FILE_LINE_COUNT INT,
    id varchar(10000),
    created_at varchar(10000),
    updated_at varchar(10000),
    user_id varchar(10000),
    user_name varchar(10000),
    name varchar(10000),
    message varchar(10000),
    content_thread_id varchar(10000),
    content_source_id varchar(10000),
    intervention_id varchar(10000),
    content_id varchar(10000),
    category_ids varchar(10000),
    task_id varchar(10000),
    action varchar(10000),
    step varchar(10000),
    initial_category_ids varchar(10000)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/DIMELO/JOURNAL'
TBLPROPERTIES ('serialization.null.format'='');