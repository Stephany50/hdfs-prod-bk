CREATE TABLE MON.SPARK_FT_CXD_NBRE_TRANSACTIONS_ECHECS(
    numero varchar(200),
    nbre_echecs_transanctions_om bigint,
    erreur_echec  varchar(1000),
    insert_date timestamp
)PARTITIONED BY (event_date DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

