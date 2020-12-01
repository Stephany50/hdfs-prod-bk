create table SPOOL.SPARK_SPOOL_SUSPENSIONS_TELCO (
    date_suspension date,
    msisdn string,
    statut_validation_bo string,
    statut_validation_boo string,
    date_activation timestamp,
    date_validation_bo timestamp,
    site_name string,
    townname string,
    administrative_region string,
    commercial_region string,
    last_location_day timestamp,
    statut_zm string,
    date_validation_bo_zm timestamp,
    statut_validation_bo_zm string,
    order_reason string,
    block_reason string,
    insert_date timestamp
) COMMENT 'spool_suspensions_telco'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')