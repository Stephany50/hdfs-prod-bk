CREATE EXTERNAL TABLE CDR.TT_GRAPH (
original_file_name VARCHAR(200),
msisdn VARCHAR(200),
name VARCHAR(1000)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/DATALAB/ALAIN/TT/GRAPH'
TBLPROPERTIES ('serialization.null.format'='');

CREATE TABLE cdr.spark_it_graph
(
original_file_name VARCHAR(200),
msisdn VARCHAR(200),
name VARCHAR(1000),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
