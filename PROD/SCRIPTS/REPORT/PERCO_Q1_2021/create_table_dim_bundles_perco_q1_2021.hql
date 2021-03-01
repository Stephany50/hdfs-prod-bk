create TABLE dim.spark_bundles_perco_q1_2021
(
    lot bigint,
    offer_code bigint,
    offer_name varchar(1000),
    type_bonus varchar(1000),
    comments varchar(1000)
)
COMMENT 'Liste des bundles de la perco Q1'
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
