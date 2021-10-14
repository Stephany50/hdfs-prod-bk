CREATE TABLE mon.spark_ft_crm_reporting 
(
    msisdn          VARCHAR(50),
    queue_type     VARCHAR(400),
    queue_name     VARCHAR(400),
    categorie           VARCHAR(400),
    TypeArticle         VARCHAR(400),
    article               VARCHAR(400),
    motif      VARCHAR(400),
    Agent       VARCHAR(400),
    cuid_agent      VARCHAR(400),
    total           BIGINT,
    semaine            VARCHAR(100),
    mois                VARCHAR(100),
    heure           VARCHAR(100),
    segment         VARCHAR(50),
    sous_segment        VARCHAR(50),
    region          VARCHAR(400),
    ville           VARCHAR(400),
    INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');

