CREATE TABLE SPOOL.SPOOL_BRI_MSISDN_ET_LOCALISATION_EN_INTERCO
(
    MSISDN  VARCHAR(26) ,
    NETWORK VARCHAR(50),
    SITE_NAME VARCHAR(100) ,
    TOWNNAME  VARCHAR(100) ,
    ADMINISTRATIVE_REGION VARCHAR(100) ,
    INSERT_DATE  TIMESTAMP
)
PARTITIONED BY (EVENT_MONTH VARCHAR(7))
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')