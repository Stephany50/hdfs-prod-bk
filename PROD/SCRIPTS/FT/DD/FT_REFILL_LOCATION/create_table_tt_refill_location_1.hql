CREATE TABLE TMP.TT_REFILL_LOCATION_1
(
    Refill_time VARCHAR(25),
    SENDER_MSISDN VARCHAR(100),
    RECEIVER_MSISDN VARCHAR(100),
    SENDER_CATEGORY VARCHAR(100),
    Refill_mean VARCHAR(100),
    refill_type VARCHAR(100),
    Refill_amount VARCHAR(100),
    site_name VARCHAR(100),
    site_appreciation VARCHAR(100)
)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')