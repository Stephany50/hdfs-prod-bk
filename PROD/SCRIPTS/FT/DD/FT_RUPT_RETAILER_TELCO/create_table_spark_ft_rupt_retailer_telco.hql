CREATE TABLE DD.SPARK_FT_RUPT_RETAILER_TELCO
(
    --EVENT_DATE        DATE,
    --EVENT_TIME        TIMESTAMP,
    MOBILE_NUMBER     VARCHAR(250),
    STOCK             DECIMAL(17, 2),
    AVG_AMOUNT_HOUR   DECIMAL(17, 2),
    RUPT_HOUR_MSISDN  INT,
    INSERT_DATE       TIMESTAMP,
    SITE_NAME         VARCHAR(250),
    CANAL             VARCHAR(100)
)

--PARTITIONED BY (EVENT_DATE DATE, EVENT_TIME VARCHAR(250))
PARTITIONED BY (EVENT_DATE DATE, EVENT_TIME TIMESTAMP)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')