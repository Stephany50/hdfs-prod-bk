CREATE EXTERNAL TABLE CDR.SPARK_TT_bundles_perco_q1_2021
(
    lot bigint,
    offer_code bigint,
    offer_name varchar(1000),
    type_bonus varchar(1000),
    comments varchar(1000)
)
COMMENT 'Table TT des bundles de la perco q1 2021'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\;'
LOCATION '/DATALAB/TEST/PERCO_Q1_2021/DIM_BUNDLES'
TBLPROPERTIES ('serialization.null.format'='')