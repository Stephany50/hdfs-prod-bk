CREATE TABLE AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2
(
    DESTINATION_CODE        VARCHAR(50),
    PROFILE_CODE            VARCHAR(50),
    SERVICE_CODE            VARCHAR(50),
    KPI                     VARCHAR(50),
    SUB_ACCOUNT             VARCHAR(50),
    MEASUREMENT_UNIT        VARCHAR(50),
    SOURCE_TABLE            VARCHAR(50),
    OPERATOR_CODE           VARCHAR(50),
    TOTAL_AMOUNT            DOUBLE,
    RATED_AMOUNT            DOUBLE,
    INSERT_DATE             TIMESTAMP,
    REGION_ID               VARCHAR(50)
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')


CREATE TABLE AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2
(
    DESTINATION_CODE        VARCHAR(50),
    PROFILE_CODE            VARCHAR(50),
    SERVICE_CODE            VARCHAR(50),
    KPI                     VARCHAR(50),
    SUB_ACCOUNT             VARCHAR(50),
    MEASUREMENT_UNIT        VARCHAR(50),
    SOURCE_TABLE            VARCHAR(50),
    OPERATOR_CODE           VARCHAR(50),
    TOTAL_AMOUNT            DOUBLE,
    RATED_AMOUNT            DOUBLE,
    INSERT_DATE             TIMESTAMP,
    REGION_ID               VARCHAR(50)
)
PARTITIONED BY (TRANSACTION_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
a.msisdn msisdn,a.service_type,a.vol vol,a.val val,a.commission commission,revenu,details,jour

CREATE TABLE AGG.SPARK_FT_A_DATAMART_OM_MARKETING3
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