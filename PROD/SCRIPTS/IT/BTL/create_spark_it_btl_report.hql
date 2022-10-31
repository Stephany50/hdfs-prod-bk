CREATE EXTERNAL TABLE cdr.tt_nomad_log (
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
version varchar(500),
action_type varchar(500),
comment varchar(500),
is_creation varchar(500),
remote_address varchar(500),
update_date varchar(500),
updater_id varchar(500),
class varchar(500),
user_id varchar(500),
contract_id varchar(500),
success varchar(500)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'
LOCATION '/PROD/TT/IDENTIFICATION/NOMAD_LOG'
TBLPROPERTIES ('serialization.null.format'='')

CREATE EXTERNAL TABLE cdr.spark_it_nomad_log (
version varchar(500),
action_type varchar(500),
comment varchar(500),
is_creation varchar(500),
remote_address varchar(500),
update_date varchar(500),
updater_id varchar(500),
class varchar(500),
user_id varchar(500),
contract_id varchar(500),
success varchar(500),
ORIGINAL_FILE_NAME VARCHAR(200),
ORIGINAL_FILE_SIZE INT,
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');