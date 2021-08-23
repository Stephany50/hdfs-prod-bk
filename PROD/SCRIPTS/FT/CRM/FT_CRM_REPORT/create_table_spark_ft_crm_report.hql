CREATE TABLE mon.spark_ft_crm_reporting 
(
msisdn       VARCHAR(400),
segment         VARCHAR(50),
sous_segment        VARCHAR(50),
Date_Interaction         VARCHAR(400),
categorie           VARCHAR(400),
typarticle              VARCHAR(400),
article               VARCHAR(400),
motif      VARCHAR(400),
total           BIGINT,
ville           VARCHAR(400),
region          VARCHAR(400),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');