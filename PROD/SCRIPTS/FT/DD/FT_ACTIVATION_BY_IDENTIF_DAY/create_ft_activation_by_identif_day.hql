CREATE TABLE MON.SPARK_FT_ACTIVATION_BY_IDENTIF_DAY
(
IDENTIFICATEUR VARCHAR(100),
MSISDN_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ACTIVATION_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')