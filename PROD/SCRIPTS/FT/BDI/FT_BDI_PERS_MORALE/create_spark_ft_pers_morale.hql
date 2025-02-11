CREATE TABLE MON.SPARK_FT_BDI_PERS_MORALE (
COMPTE_CLIENT    varchar(300),
RAISON_SOCIALE    varchar(300),
NOM_REPRESENTANT_LEGAL    varchar(300),
PRENOM_REPRESENTANT_LEGAL    varchar(300),
CNI_REPRESENTANT_LOCAL    varchar(300),
CONTACT_TELEPHONIQUE    varchar(300),
VILLE_STRUCTURE    varchar(300),
QUARTIER_STRUCTURE    varchar(300),
ADRESSE_STRUCTURE    varchar(300),
NUMERO_REGISTRE_COMMERCE    varchar(300),
SMS_CONTACT    varchar(300),
DOC_PLAN_LOCALISATION    varchar(300),
DOC_FICHE_SOUSCRIPTION    varchar(300),
ACCEPTATION_CGV    varchar(300),
DOC_ATTESTATION_CNPS    varchar(300),
DOC_RCCM    varchar(300),
DISPONIBILITE_SCAN    varchar(300),
type_client varchar(100),
RAISON_SOCIALE_AN  varchar(100),
RCCM_AN  varchar(100),
CNI_REPRESENTANT_LEGAL_AN varchar(100),
ADRESSE_STRUCTURE_AN varchar(100),
INSERT_DATE TIMESTAMP
)
PARTITIONED BY (EVENT_DATE DATE)
STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESS'='SNAPPY');