CREATE TABLE MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
(
    sender_msisdn varchar(100),
    receiver_msisdn varchar(100),
    event_hour varchar(2),
    transaction_amount decimal(17, 2),
    revenu_om decimal(17, 2),
    service_type varchar(100),
    site_name varchar(1000),
    town varchar(1000),
    region varchar(1000),
    commercial_region varchar(1000),
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')