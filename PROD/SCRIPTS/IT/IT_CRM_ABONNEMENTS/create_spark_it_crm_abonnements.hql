create table CDR.SPARK_IT_CRM_ABONNEMENTS (
ID  INTEGER,
COMPTE_B2B  VARCHAR (100),
COMPTE_B2C  VARCHAR (100),
COMPTE_FACTURATION  VARCHAR (100),
TYPE_ABONNEMENT  INTEGER,
LBL_TYPE_ABONNEMENT  VARCHAR (100),
MSISDN_IDWIMAX  VARCHAR (100),
ICCI_ADRESSE_MAC  VARCHAR (100),
IMSI  VARCHAR (100),
CODE_PUK1  VARCHAR (100),
CODE_PUK2  VARCHAR (100),
DATE_ACTIVATION  TIMESTAMP,
STATUT_ABONNEMENT  INTEGER,
LBL_STATUT_ABONNEMENT  VARCHAR (100),
DATE_MAJ_STATUT  TIMESTAMP,
OFFRE  INTEGER,
LBL_OFFRE  VARCHAR (100),
PROPRIETAIRE  VARCHAR (100),
LBL_PROPRIETAIRE  VARCHAR (100),
ORIGINAL_FILE_NAME  VARCHAR (100),
INSERTED_DATE  TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')