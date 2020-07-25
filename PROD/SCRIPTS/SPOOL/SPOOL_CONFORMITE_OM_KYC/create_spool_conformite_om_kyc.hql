create table SPOOL.SPOOL_CONFORMITE_OM_KYC (
nom                       varchar(200),
prenom                    varchar(200),
date_naissance            date ,
numero_cni                varchar(50) ,
msisdn                    varchar(50),
EST_ACTIF_OM_90J    char(3),
EST_ACTIF_OM_30J    char(3),
activation_date_tel       timestamp  ,
created_date_om           timestamp ,
insert_date               timestamp
)  COMMENT 'SPOOL_CONFORMITE_OM_KYC'
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')