CREATE TABLE AGG.SPARK_FT_BTL_SUBS
(
type_forfait varchar(500),
msisdn_vendeur varchar(200),
ipp_name varchar(500),
ipp_stream varchar(500),
site_name varchar(500),
site_name_vendeur varchar(500),
msisdn_count bigint,
nb_sous bigint,
ca decimal(17, 2),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')


