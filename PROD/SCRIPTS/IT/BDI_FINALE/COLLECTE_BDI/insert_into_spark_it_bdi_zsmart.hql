INSERT INTO CDR.SPARK_IT_BDI_ZSMART
select
MSISDN  ,
ID_TYPE_PIECE  ,
NUMERO_PIECE  ,
DATE_EXPIRATION  ,
DATE_NAISSANCE  ,
DATE_ACTIVATION  ,
ADRESSE  ,
QUARTIER  ,
VILLE  ,
STATUT  ,
STATUT_VALIDATION_BO  ,
MOTIF_REJET_BO  ,
DATE_VALIDATION_BO  ,
LOGIN_VALIDATEUR_BO  ,
CANAL_VALIDATEUR_BO  ,
TYPE_ABONNEMENT  ,
CSMODDATE  ,
CCMODDATE  ,
COMPTE_CLIENT_STRUCTURE  ,
NOM_STRUCTURE  ,
NUMERO_REGISTRE_COMMERCE  ,
NUMERO_PIECE_REPRESENTANT_LEGAL  ,
DATE_SOUSCRIPTION  ,
DATE_CHANGEMENT_STATUT  ,
VILLE_STRUCTURE  ,
QUARTIER_STRUCTURE  ,
RAISON_STATUT  ,
PRENOM  ,
NOM  ,
CUSTOMER_ID  ,
CONTRACT_ID  ,
COMPTE_CLIENT  ,
PLAN_LOCALISATION  ,
CONTRAT_SOUCRIPTION  ,
ACCEPTATION_CGV  ,
DISPONIBILITE_SCAN  ,
NOM_TUTEUR  ,
PRENOM_TUTEUR  ,
DATE_NAISSANCE_TUTEUR  ,
NUMERO_PIECE_TUTEUR  ,
DATE_EXPIRATION_TUTEUR  ,
ID_TYPE_PIECE_TUTEUR  ,
ADRESSE_TUTEUR  ,
IDENTIFICATEUR  ,
LOCALISATION_IDENTIFICATEUR  ,
PROFESSION  ,
ORIGINAL_FILE_NAME,
ORIGINAL_FILE_SIZE,
ORIGINAL_FILE_LINE_COUNT,
CURRENT_TIMESTAMP() INSERT_DATE,
TO_DATE(FROM_UNIXTIME(UNIX_TIMESTAMP(SUBSTRING (ORIGINAL_FILE_NAME, -12, 8),'yyyyMMdd'))) ORIGINAL_FILE_DATE
FROM CDR.TT_ZSMARTTOBDI C
LEFT JOIN (SELECT DISTINCT  ORIGINAL_FILE_NAME FILE_NAME FROM CDR.SPARK_IT_BDI_ZSMART) T ON T.FILE_NAME=C.ORIGINAL_FILE_NAME
WHERE T.FILE_NAME IS NULL