CREATE TABLE MON.SPARK_FT_REPORTING_DIMELO
(
    created_at DATE,
    periode varchar(10),
    user_id varchar(1000),
    user_name varchar(1000),
    expired_sum int,
    transferred_sum int,
    terminated_sum int,
    deferred_sum int,
    restablished_sum int,
    assignes_sum int ,
    acceptes_sum int,
    missed_sum int,
    team varchar(1000),
    channel varchar(1000)
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY");
