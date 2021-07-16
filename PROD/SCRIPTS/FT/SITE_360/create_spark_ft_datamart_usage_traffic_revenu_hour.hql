CREATE TABLE MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
(
    
    msisdn varchar(100),
    event_hour varchar(2),
    device_type varchar(1000),
    techno_device varchar(20),
    trafic_voix decimal(17, 2),
    trafic_data decimal(17, 2),
    trafic_sms decimal(17, 2),
    revenu_voix_pyg decimal(17, 2),
    revenu_voix_subs decimal(17, 2),
    revenu_data decimal(17, 2),
    revenu_sms_pyg decimal(17, 2),
    revenu_sms_subs decimal(17, 2),
    site_name varchar(1000),
    town varchar(1000),
    region varchar(1000),
    commercial_region varchar(1000),
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

alter table MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR add columns(est_parc_groupe varchar(3))
