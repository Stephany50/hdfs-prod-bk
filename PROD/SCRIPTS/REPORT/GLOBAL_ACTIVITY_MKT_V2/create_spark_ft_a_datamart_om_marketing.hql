CREATE TABLE AGG.SPARK_FT_A_DATAMART_OM_MARKETING
(
    administrative_region        VARCHAR(50),
    PROFILE_CODE            VARCHAR(50),
    details            VARCHAR(50),
    val                     DOUBLE,
    vol             DOUBLE,
    revenu       DOUBLE,
    commission         DOUBLE,
    nb_lignes INT,
    nb_numeros INT,
    style VARCHAR(50),
    service_type VARCHAR(50),
    OPERATOR_CODE           VARCHAR(50),
    INSERT_DATE             TIMESTAMP
)
PARTITIONED BY (JOUR DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')