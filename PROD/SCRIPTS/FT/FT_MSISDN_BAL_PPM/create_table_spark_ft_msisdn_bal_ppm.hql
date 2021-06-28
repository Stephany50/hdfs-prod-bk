
CREATE TABLE MON.spark_ft_msisdn_bal_ppm
(
    msisdn varchar(100),
    bal_id bigint,
    ppm decimal(25, 2),
    revenu_already_dispatched decimal(25, 2), -- Svalorisé[i-1]
    insert_date timestamp
)
partitioned by (event_date date)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')