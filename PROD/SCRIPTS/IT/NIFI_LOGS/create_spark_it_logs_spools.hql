
CREATE TABLE cdr.spark_it_logs_spools (
    filename varchar(200),
    filesize bigint,
    filecount bigint,
    filetype varchar(200),
    merged_filename varchar(200),
    fluxtype varchar(50),
    provenance varchar(50),
    status varchar(50),
    log_datetime TIMESTAMP,
    flowfile_attr varchar(2000),
    insert_date TIMESTAMP
)
PARTITIONED BY (LOG_DATE DATE)
CLUSTERED BY(fluxtype,provenance, status) INTO 4 BUCKETS
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")

