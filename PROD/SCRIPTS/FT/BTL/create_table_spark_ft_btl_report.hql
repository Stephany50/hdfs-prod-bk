CREATE TABLE MON.SPARK_FT_BTL_REPORT
(
MSISDN VARCHAR(500),
type_forfait varchar(500),
msisdn_vendeur varchar(500),
prix decimal(17, 2),
ipp varchar(500),
ipp_name varchar(2000),
ipp_stream varchar(100),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')


