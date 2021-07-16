CREATE TABLE MON.SPARK_FT_CUSTOMER_BASE (
    MSISDN VARCHAR(40),
    parc_group INT,
    parc_art INT,
    parc_om INT,
    parc_actif_om INT,
    all30daysbase INT,
    dailybase INT,
    all90dayslost INT,
    all30dayslost INT,
    lostat90days INT,
    lostat30days INT,
    all90dayswinback INT,
    all30dayswinback INT,
    winbackat90days INT,
    winbackat30days INT,
    churner INT,
    gross_add INT,
    
    gross_add_om INT,
    smartphone_user INT,
    segment_valeur_premium VARCHAR(400),
    segment_valeur_high_value VARCHAR(400),
    segment_valeur_telco VARCHAR(400),
    type_usage VARCHAR(400),
    anciennete VARCHAR(400),
    statut_urbanite  VARCHAR(400),
	site_name  VARCHAR(400),
	ville  VARCHAR(400),
	REGION_ADMINISTRATIVE  VARCHAR(400),
    REGION_COMMERCIAL  VARCHAR(400),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET
TBLPROPERTIES ("parquet.compress"="SNAPPY")
