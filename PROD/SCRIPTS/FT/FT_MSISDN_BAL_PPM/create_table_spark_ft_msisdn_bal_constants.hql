CREATE TABLE MON.spark_ft_msisdn_bal_constants
(
    msisdn varchar(100),
    bal_id bigint,
    bal_revenu bigint,
    bal_life_time bigint,
    index_of_day bigint,
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')


CREATE TABLE MON.spark_ft_msisdn_bal_constants_new
(
    msisdn varchar(100),
    bal_id bigint,
    bal_revenu bigint,
    bal_life_time bigint,
    index_of_day bigint,
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')