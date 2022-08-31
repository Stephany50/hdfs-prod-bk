create table mon.spark_ft_usage_horaire_localise
(
    msisdn varchar(100)
    , hour_period varchar(2)
    , last_lac varchar(5)
    , last_ci varchar(5)
    , bal_id bigint
    , acct_res_id int
    , acct_res_name varchar(50)
    , acct_item_type_id int
    , service varchar(10)
    , usage_horaire DECIMAL(30, 8)
    , insert_date timestamp
)
partitioned by (event_date date)
stored as parquet
tblproperties ('PARQUET.COMPRESS'='SNAPPY')
