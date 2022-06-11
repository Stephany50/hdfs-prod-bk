CREATE TABLE MON.SPARK_FT_BTL_DATA_DETAIL
(
MSISDN VARCHAR(500),
datauser_type varchar(500),
site_name varchar(500),
site_name_vendeur varchar(500),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

