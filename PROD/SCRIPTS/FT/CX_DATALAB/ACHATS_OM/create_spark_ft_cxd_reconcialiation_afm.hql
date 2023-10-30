CREATE TABLE MON.SPARK_FT_CXD_RECONCIALIATION_AFM(
    numero varchar(200),
    transfer_id varchar(200),
    date_debit timestamp,
    date_depot timestamp,
    insert_date timestamp
)PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');


