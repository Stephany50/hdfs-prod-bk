
create table AGG.SPARK_FT_A_PARC_OM (
    administrative_region  VARCHAR(100),
    service_type  VARCHAR(100),
    parc_om_30j  INT,
    details  VARCHAR(100),
    operator_code VARCHAR(100),
    profile_code VARCHAR(100),
    INSERT_DATE TIMESTAMP
)PARTITIONED BY (    JOUR DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
