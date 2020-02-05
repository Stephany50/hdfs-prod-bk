
CREATE TABLE DIM.SPARK_DT_SERVICE_CENTER (
    global_title    bigint,
    carrier    varchar(250),
    pays    varchar(250),
    prefix    int,
    insert_date    timestamp,
    type_flux    varchar(20)
)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
