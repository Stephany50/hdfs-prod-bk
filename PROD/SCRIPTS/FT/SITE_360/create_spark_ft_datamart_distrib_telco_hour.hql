CREATE TABLE MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
(
    msisdn varchar(100),
    event_hour varchar(2),
    refill_amount decimal(17, 2),
    category_domain varchar(100),
    site_name varchar(1000),
    town varchar(1000),
    region varchar(1000),
    commercial_region varchar(1000),
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')