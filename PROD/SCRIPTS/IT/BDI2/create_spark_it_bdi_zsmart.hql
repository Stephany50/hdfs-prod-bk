CREATE TABLE CDR.SPARK_IT_BDI_ZSMART  (
MSISDN  varchar(100),
ID_TYPE_PIECE  varchar(100),
NUMERO_PIECE  varchar(100),
DATE_EXPIRATION  varchar(100),
DATE_NAISSANCE  varchar(100),
DATE_ACTIVATION  varchar(100),
ADRESSE  varchar(100),
QUARTIER  varchar(100),
VILLE  varchar(100),
STATUT  varchar(100),
STATUT_VALIDATION_BO  varchar(100),
MOTIF_REJET_BO  varchar(100),
DATE_VALIDATION_BO  varchar(100),
LOGIN_VALIDATEUR_BO  varchar(100),
CANAL_VALIDATEUR_BO  varchar(100),
TYPE_ABONNEMENT  varchar(100),
CSMODDATE  varchar(100),
CCMODDATE  varchar(100),
COMPTE_CLIENT_STRUCTURE  varchar(100),
NOM_STRUCTURE  varchar(100),
NUMERO_REGISTRE_COMMERCE  varchar(100),
NUMERO_PIECE_REPRESENTANT_LEGAL  varchar(100),
DATE_SOUSCRIPTION  varchar(100),
DATE_CHANGEMENT_STATUT  varchar(100),
VILLE_STRUCTURE  varchar(100),
QUARTIER_STRUCTURE  varchar(100),
RAISON_STATUT  varchar(100),
PRENOM  varchar(100),
NOM  varchar(100),
CUSTOMER_ID  varchar(100),
CONTRACT_ID  varchar(100),
COMPTE_CLIENT  varchar(100),
PLAN_LOCALISATION  varchar(100),
CONTRAT_SOUCRIPTION  varchar(100),
ACCEPTATION_CGV  varchar(100),
DISPONIBILITE_SCAN  varchar(100),
NOM_TUTEUR  varchar(100),
PRENOM_TUTEUR  varchar(100),
DATE_NAISSANCE_TUTEUR  varchar(100),
NUMERO_PIECE_TUTEUR  varchar(100),
DATE_EXPIRATION_TUTEUR  varchar(100),
ID_TYPE_PIECE_TUTEUR  varchar(100),
ADRESSE_TUTEUR  varchar(100),
IDENTIFICATEUR  varchar(100),
LOCALISATION_IDENTIFICATEUR  varchar(100),
PROFESSION  varchar(100),
ORIGINAL_FILE_NAME    VARCHAR(50),
ORIGINAL_FILE_SIZE    VARCHAR(50),
ORIGINAL_FILE_LINE_COUNT INT,
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (ORIGINAL_FILE_DATE DATE)
CLUSTERED BY(MSISDN) INTO 64 BUCKETS
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY')