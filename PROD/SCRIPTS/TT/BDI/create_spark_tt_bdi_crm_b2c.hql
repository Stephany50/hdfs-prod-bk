CREATE EXTERNAL  TABLE CDR.tt_bdi_crm_b2c(
MSISDN   varchar(255),
TYPE_PERSONNE   varchar(255),
NOM_PRENOM   varchar(255),
ID_TYPE_PIECE   varchar(64),
TYPE_PIECE   varchar(64),
NUMERO_PIECE   varchar(255),
DATE_EXPIRATION   varchar(255),
DATE_NAISSANCE   varchar(255),
DATE_ACTIVATION   varchar(255),
ADRESSE   varchar(255),
QUARTIER   varchar(255),
VILLE   varchar(255),
STATUT   varchar(255),
STATUT_VALIDATION_BO   varchar(5),
MOTIF_REJET_BO   varchar(255),
DATE_VALIDATION_BO   varchar(255),
LOGIN_VALIDATEUR_BO   varchar(255),
CANAL_VALIDATEUR_BO   varchar(255),
TYPE_ABONNEMENT   varchar(255),
CSMODDATE   varchar(255),
CCMODDATE   varchar(255),
COMPTE_CLIENT_STRUCTURE   varchar(255),
NOM_STRUCTURE   varchar(255),
NUMERO_REGISTRE_COMMERCE   varchar(255),
NUMERO_PIECE_REPRESENTANT_LEGAL   varchar(255),
IMEI   varchar(255),
STATUT_DEROGATION   varchar(5),
REGION_ADMINISTRATIVE   varchar(255),
REGION_COMMERCIALE   varchar(255),
SITE_NAME   varchar(255),
VILLE_SITE   varchar(255),
OFFRE_COMMERCIALE   varchar(255),
TYPE_CONTRAT   varchar(255),
SEGMENTATION   varchar(255),
DATE_SOUSCRIPTION   varchar(255),
DATE_CHANGEMENT_STATUT   varchar(255),
VILLE_STRUCTURE   varchar(255),
QUARTIER_STRUCTURE   varchar(255),
RAISON_STATUT   varchar(255),
PRENOM   varchar(255),
NOM   varchar(255),
CUSTOMER_ID   varchar(255),
CONTRACT_ID   varchar(255),
COMPTE_CLIENT   varchar(255),
PLAN_LOCALISATION   varchar(255),
CONTRAT_SOUCRIPTION   varchar(255),
ACCEPTATION_CGV   varchar(255),
DISPONIBILITE_SCAN   varchar(255),
NOM_TUTEUR   varchar(255),
PRENOM_TUTEUR   varchar(255),
DATE_NAISSANCE_TUTEUR   varchar(255),
NUMERO_PIECE_TUTEUR   varchar(255),
DATE_EXPIRATION_TUTEUR   varchar(255),
ID_TYPE_PIECE_TUTEUR   varchar(255),
TYPE_PIECE_TUTEUR   varchar(255),
ADRESSE_TUTEUR   varchar(255),
IDENTIFICATEUR   varchar(64),
LOCALISATION_IDENTIFICATEUR   varchar(255),
PROFESSION   varchar(255)
)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/PROD/TT/BDI/B2B/'
TBLPROPERTIES ('serialization.null.format'='');