CREATE EXTERNAl TABLE CDR.tt_zsmarttobdi(
MSISDN  varchar(255),
ID_TYPE_PIECE  varchar(255),
NUMERO_PIECE  varchar(255),
DATE_EXPIRATION  varchar(255),
DATE_NAISSANCE  varchar(255),
DATE_ACTIVATION  varchar(255),
ADRESSE  varchar(255),
QUARTIER  varchar(255),
VILLE  varchar(255),
STATUT  varchar(255),
STATUT_VALIDATION_BO  varchar(255),
MOTIF_REJET_BO  varchar(255),
DATE_VALIDATION_BO  varchar(255),
LOGIN_VALIDATEUR_BO  varchar(255),
CANAL_VALIDATEUR_BO  varchar(255),
TYPE_ABONNEMENT  varchar(255),
CSMODDATE  varchar(255),
CCMODDATE  varchar(255),
COMPTE_CLIENT_STRUCTURE  varchar(255),
NOM_STRUCTURE  varchar(255),
NUMERO_REGISTRE_COMMERCE  varchar(255),
NUMERO_PIECE_REPRESENTANT_LEGAL  varchar(255),
DATE_SOUSCRIPTION  varchar(255),
DATE_CHANGEMENT_STATUT  varchar(255),
VILLE_STRUCTURE  varchar(255),
QUARTIER_STRUCTURE  varchar(255),
RAISON_STATUT  varchar(255),
PRENOM  varchar(255),
NOM  varchar(255),
CUSTOMER_ID  varchar(255),
CONTRACT_ID  varchar(255),
COMPTE_CLIENT  varchar(255),
PLAN_LOCALISATION  varchar(255),
CONTRAT_SOUCRIPTION  varchar(255),
ACCEPTATION_CGV  varchar(255),
DISPONIBILITE_SCAN  varchar(255),
NOM_TUTEUR  varchar(255),
PRENOM_TUTEUR  varchar(255),
DATE_NAISSANCE_TUTEUR  varchar(255),
NUMERO_PIECE_TUTEUR  varchar(255),
DATE_EXPIRATION_TUTEUR  varchar(255),
ID_TYPE_PIECE_TUTEUR  varchar(255),
ADRESSE_TUTEUR  varchar(255),
IDENTIFICATEUR  varchar(255),
LOCALISATION_IDENTIFICATEUR  varchar(255),
PROFESSION  varchar(255)

)
COMMENT 'external tables-TT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/PROD/TT/ZSMART/ZSMARTTOBDI'
TBLPROPERTIES ('serialization.null.format'='');