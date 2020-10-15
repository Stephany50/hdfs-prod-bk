create table tmp.SPARK_KPIS_REG2 (
    region_administrative  VARCHAR(100),
    region_commerciale  VARCHAR(100),
    category  VARCHAR(100),
    kpi  VARCHAR(100),
    axe_revenue VARCHAR(100),
    axe_subscriber VARCHAR(100),
    axe_regionale VARCHAR(100),
    granularite VARCHAR(100),
    valeur DOUBLE
)PARTITIONED BY (    processing_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

create table tmp.SPARK_KPIS_REG3 (
    region_administrative  VARCHAR(100),
    region_commerciale  VARCHAR(100),
    category  VARCHAR(100),
    kpi  VARCHAR(100),
    axe_revenue VARCHAR(100),
    axe_subscriber VARCHAR(100),
    axe_regionale VARCHAR(100),
    granularite VARCHAR(100),
    valeur DOUBLE
)PARTITIONED BY (    processing_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

create table tmp.SPARK_KPIS_REG5 (
    region_administrative  VARCHAR(100),
    region_commerciale  VARCHAR(100),
    category  VARCHAR(100),
    kpi  VARCHAR(100),
    axe_revenue VARCHAR(100),
    axe_subscriber VARCHAR(100),
    axe_regionale VARCHAR(100),
    granularite VARCHAR(100),
    valeur DOUBLE,
    cummulable VARCHAR(10)
)PARTITIONED BY (    processing_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')
