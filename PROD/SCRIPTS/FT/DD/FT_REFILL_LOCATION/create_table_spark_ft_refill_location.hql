CREATE TABLE mon.spark_ft_refill_location
(
    Refill_time VARCHAR(25),
    SENDER_MSISDN VARCHAR(100),
    RECEIVER_MSISDN VARCHAR(100),
    SENDER_CATEGORY VARCHAR(100),
    Refill_mean VARCHAR(100),
    refill_type VARCHAR(100),
    Refill_amount DOUBLE,
    site_name_sender VARCHAR(100),
    site_appreciation_sender VARCHAR(100),
    site_name_receiver VARCHAR(100),
    site_appreciation_receiver VARCHAR(100),
    insert_date TIMESTAMP
)
PARTITIONED BY (refill_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')



