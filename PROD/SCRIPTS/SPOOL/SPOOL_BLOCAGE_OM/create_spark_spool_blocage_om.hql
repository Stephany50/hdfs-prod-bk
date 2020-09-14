create table SPOOL.SPARK_SPOOL_BLOCAGE_OM (
    msisdn string,
    nom string,
    prenom string,
    user_domain  string,
    registered_on timestamp,
    balance decimal(17,2),
    account_type  string,
    account_name  string,
    account_id    string,
    insert_date timestamp
) COMMENT 'SPOOL_BLOCAGE_OM'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')