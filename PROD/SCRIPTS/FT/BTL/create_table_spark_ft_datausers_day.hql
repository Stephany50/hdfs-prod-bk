CREATE TABLE MON.SPARK_FT_DATAUSERS_DAY
(
MSISDN VARCHAR(200),
segment_data varchar(200),
last_day_month date,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (event_month varchar(200))
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
