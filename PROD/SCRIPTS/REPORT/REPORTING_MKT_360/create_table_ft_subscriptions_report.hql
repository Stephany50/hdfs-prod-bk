CREATE TABLE MON.spark_ft_subscriptions_report (
    ipp VARCHAR(400),
    kpi_name VARCHAR(1000),
    kpi_value DECIMAL(17, 2),
    statut_urbanite VARCHAR(1000),
    site_name VARCHAR(400),
    ville VARCHAR(400),
    region_administrative VARCHAR(400),
    REGION_COMMERCIAL VARCHAR(400),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")
