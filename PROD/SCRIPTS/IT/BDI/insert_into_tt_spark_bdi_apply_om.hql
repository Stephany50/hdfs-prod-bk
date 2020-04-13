INSERT INTO TABLE TMP.TT_SPARK_IT_BDI_TMP_APPLY_OM1
SELECT
MSISDN,
CUSTOMER_ID,
CONTRACT_ID,
COMPTE_CLIENT,
TYPE_PERSONNE, IF(i.TYPE_PIECE = '1', 'CNI',
IF(i.TYPE_PIECE = '2' OR i.TYPE_PIECE = '3', 'PASSEPORT', IF(i.TYPE_PIECE = '4', 'CARTE_SEJOUR', IF(i.TYPE_PIECE = '5', 'COMPTE_CONTRIBUABLE', IF(i.TYPE_PIECE = '6', 'NUMERO_REGISTRE_COMMERCE', IF(i.TYPE_PIECE = '7', 'CARTE_SCOLAIRE', IF(i.TYPE_PIECE = '8', 'AUTRE', IF(i.TYPE_PIECE = '9','RECEPISSE','CNI')))))))) AS TYPE_PIECE,
NUMERO_PIECE,
TYPE_PIECE AS ID_TYPE_PIECE,
NOM_PRENOM,
NOM,
PRENOM,
DATE_NAISSANCE,
DATE_EXPIRATION,
ADRESSE,
VILLE,
QUARTIER,
DATE_SOUSCRIPTION,
DATE_ACTIVATION,
STATUT,
RAISON_STATUT,
DATE_CHANGEMENT_STATUT,
PLAN_LOCALISATION,
CONTRAT_SOUCRIPTION,
DISPONIBILITE_SCAN,
ACCEPTATION_CGV,
TYPE_PIECE_TUTEUR,
NUMERO_PIECE_TUTEUR,
NOM_PRENOM_TUTEUR AS NOM_TUTEUR,
DATE_NAISSANCE_TUTEUR,
DATE_EXPIRATION_TUTEUR,
ADRESSE_TUTEUR,
NOM_STRUCTURE,
NUMERO_REGISTRE_COMMERCE,
NUMERO_PIECE_REPRESENTANT_LEGAL,

'' as  IMEI ,
'' as  STATUT_DEROGATION ,
'' as  REGION_ADMINISTRATIVE ,
'' as  REGION_COMMERCIALE  ,
'' as  SITE_NAME ,
'' as  VILLE_SITE  ,
'' as OFFRE_COMMERCIALE   ,
'' as TYPE_CONTRAT ,
'' as SEGMENTATION  ,
CURRENT_TIMESTAMP INSERT_DATE
 FROM cdr.it_identification
 GROUP BY MSISDN