CREATE TABLE  MON.SPARK_SMS_SPOOL (
 MSISDN STRING,
 SMS STRING,
 INSERT_DATE TIMESTAMP
)
    PARTITIONED BY (TRANSACTION_DATE DATE)
    STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');