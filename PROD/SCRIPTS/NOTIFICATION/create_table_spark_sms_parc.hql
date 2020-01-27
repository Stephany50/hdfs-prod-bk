
CREATE TABLE IF NOT EXISTS MON.SPARK_SMS_PARC(
    MSISDN STRING,
    SMS STRING,
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (SDATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');
