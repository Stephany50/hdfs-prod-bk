
CREATE TABLE MON.spark_ft_msisdn_bal_usage_day
(
    msisdn varchar(100),
    bal_id bigint,
    sum_conso_until_day bigint,
    sum_conso_until_yesterday bigint,
    conso_of_yesterday bigint,
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')



CREATE TABLE MON.spark_ft_msisdn_bal_usage_day_new
(
    msisdn varchar(100),
    bal_id bigint,
    service varchar(100),
    sum_conso_until_day bigint,
    sum_conso_until_yesterday bigint,
    conso_of_yesterday bigint,
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')