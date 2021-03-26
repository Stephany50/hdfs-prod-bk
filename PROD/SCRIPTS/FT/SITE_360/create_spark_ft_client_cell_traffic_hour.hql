CREATE TABLE MON.SPARK_FT_CLIENT_CELL_TRAFFIC_HOUR
(
    msisdn varchar(100),
    hour_period varchar(2),
    --identificateur varchar(100),
    --contract_type varchar(100),
    --commercial_offer varchar(100),
    --activation_date date,
    --deactivation_date date,
    --est_parc_art varchar(5),
    --est_parc_groupe varchar(5),
    last_transaction_date_time timestamp,
    last_transaction_type varchar(5),
    last_ci varchar(5),
    last_lac varchar(5),
    --site_name varchar(1000),
    --town varchar(1000),
    --region varchar(1000),
    --commercial_region varchar(1000),
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')