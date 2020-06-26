CREATE TABLE SPOOL.spool_bri_msisdn_et_localisation_en_interco
(
msisdn  varchar(26) ,
network varchar(50),
site_name varchar(100) ,
townname  varchar(100) ,
administrative_region varchar(100) ,
insert_date  timestamp
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')