CREATE TABLE MON.SPARK_FT_MSISDN_IMEI_LOCALISATION_TO_BDI (
MSISDN   varchar(255),
IMEI   varchar(255),
SITE_NAME   varchar(255),
COMMERCIAL_REGION   varchar(255),
ADMINISTRATIVE_REGION   varchar(255),
TOWNNAME   varchar(255),
COMMERCIAL_OFFER   varchar(255),
CONTRACT_TYPE   varchar(255),
SEGMENTATION   varchar(255),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');