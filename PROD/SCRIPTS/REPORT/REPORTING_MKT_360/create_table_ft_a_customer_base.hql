CREATE TABLE AGG.SPARK_FT_A_CUSTOMER_BASE (
    parc_group BIGINT,
    parc_art BIGINT,
    parc_om BIGINT,
    parc_actif_om BIGINT,
    all30daysbase BIGINT,
    dailybase BIGINT,
    all90dayslost BIGINT,
    all30dayslost BIGINT,
    lostat90days BIGINT,
    lostat30days BIGINT,
    all90dayswinback BIGINT,
    all30dayswinback BIGINT,
    winbackat90days BIGINT,
    winbackat30days BIGINT,
    churner BIGINT,
    gross_add BIGINT,
    gross_add_om BIGINT,
    smartphone_user BIGINT,
    segment_valeur_premium VARCHAR(400),
    segment_valeur_high_value VARCHAR(400),
    segment_valeur_telco VARCHAR(400),
    type_usage VARCHAR(400),
    anciennete VARCHAR(400),
    statut_urbanite  VARCHAR(400),
	site_name  VARCHAR(400),
	ville  VARCHAR(400),
	region_administrative  VARCHAR(400),
    region_commercial varchar(400),
    INSERT_DATE TIMESTAMP
    
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")
