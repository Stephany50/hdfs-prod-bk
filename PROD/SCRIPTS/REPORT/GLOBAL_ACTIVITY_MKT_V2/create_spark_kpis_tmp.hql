
create table AGG.SPARK_KPIS_DG_TMP (
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
