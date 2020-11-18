create table AGG.SPARK_KPIS_DG_FINAL (
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
    valeur_budget DOUBLE,
    valeur_budget_mtd DOUBLE,
    valeur_mtd_last_year DOUBLE,
    valeur_vs_lweek DOUBLE,
    valeur_vs_2wa DOUBLE,
    valeur_vs_3wa DOUBLE,
    valeur_vs_4wa DOUBLE,
    valeur_mtd_vs_lmdt DOUBLE,
    valeur_mtd_vs_budget DOUBLE,
    valeur_vs_budget DOUBLE,
    valeur_mtd_vs_last_year DOUBLE,
    insert_date TIMESTAMP
)PARTITIONED BY (    processing_date DATE)
STORED AS PARQUET
TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')

