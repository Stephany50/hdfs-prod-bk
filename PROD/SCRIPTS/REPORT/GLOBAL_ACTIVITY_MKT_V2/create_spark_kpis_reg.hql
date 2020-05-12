create table mon.SPARK_KPIS_REG (
    region_administrative  VARCHAR(100),
    region_commerciale  VARCHAR(100),
    category  VARCHAR(100),
    kpi  VARCHAR(100),
    axe_revenue VARCHAR(100),
    axe_subscriber VARCHAR(100),
    axe_regionale VARCHAR(100),
    SOURCE_TABLE VARCHAR(100),
    valeur DOUBLE,
    valeur_lweek DOUBLE,
    valeur_2wa DOUBLE,
    valeur_3wa DOUBLE,
    valeur_4wa DOUBLE,
    valeur_mtd DOUBLE,
    valeur_lmtd DOUBLE,
    valeur_mtd_vs_lmdt DOUBLE,
    valeur_mtd_last_year DOUBLE,
    valeur_mtd_vs_budget DOUBLE,
    insert_date DATE
)PARTITIONED BY (    processing_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')