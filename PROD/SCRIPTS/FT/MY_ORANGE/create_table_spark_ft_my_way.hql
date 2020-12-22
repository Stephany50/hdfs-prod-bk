CREATE TABLE MON.SPARK_FT_MY_WAY (
    site_name varchar(200),
    subs_global_daily bigint,
    subs_myway_plus_daily bigint,
    subs_myway_ussd_daily bigint,
    revenu_global_daily decimal(17, 2),
    revenu_myway_plus_daily decimal(17, 2),
    revenu_myway_ussd_daily decimal(17, 2),
    insert_date TIMESTAMP
)
COMMENT 'Table for My Way KPIs'
PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
