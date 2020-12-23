INSERT INTO TMP.TT_BDI_FLOTTE
SELECT
c.MSISDN_s AS MSISDN,
b.CUSTOMER_ID AS CUSTOMER_ID,
b.CONTRACT_ID AS CONTRACT_ID,
b.COMPTE_CLIENT AS COMPTE_CLIENT,
'M2M'  AS TYPE_PERSONNE,
null AS TYPE_PIECE,
null AS NUMERO_PIECE,
null AS ID_TYPE_PIECE,
null AS NOM_PRENOM,
null AS NOM,
null AS PRENOM,
null AS DATE_NAISSANCE,
null AS DATE_EXPIRATION,
null AS ADRESSE,
null AS VILLE,
null AS QUARTIER,
null AS DATE_SOUSCRIPTION,
b.DATE_ACTIVATION AS DATE_ACTIVATION,
b.STATUT AS STATUT,
b.RAISON_STATUT AS RAISON_STATUT,
b.DATE_CHANGEMENT_STATUT AS DATE_CHANGEMENT_STATUT,
null AS PLAN_LOCALISATION,
null AS CONTRAT_SOUCRIPTION,
null AS DISPONIBILITE_SCAN,
null AS ACCEPTATION_CGV,
null AS TYPE_PIECE_TUTEUR,
null AS NUMERO_PIECE_TUTEUR,
null AS NOM_TUTEUR,
null AS PRENOM_TUTEUR,
null AS DATE_NAISSANCE_TUTEUR,
null AS DATE_EXPIRATION_TUTEUR,
null AS ADRESSE_TUTEUR,
b.COMPTE_CLIENT_STRUCTURE AS COMPTE_CLIENT_STRUCTURE,
c.NOM_STRUCTURE AS NOM_STRUCTURE,
c.NUMERO_REGISTRE_COMMERCE AS NUMERO_REGISTRE_COMMERCE,
c.NUMERO_PIECE_REPRESENTANT_LEGAL AS NUMERO_PIECE_REPRESENTANT_LEGAL,
b.IMEI AS IMEI,
null AS STATUT_DEROGATION,
null AS REGION_ADMINISTRATIVE,
null AS REGION_COMMERCIALE,
null AS SITE_NAME,
null AS VILLE_SITE,
null AS OFFRE_COMMERCIALE,
b.TYPE_CONTRAT AS TYPE_CONTRAT,
b.SEGMENTATION AS SEGMENTATION,
b.odbIncomingCalls AS odbIncomingCalls,
b.odbOutgoingCalls AS odbOutgoingCalls,
'N' AS DEROGATION_IDENTIFICATION
FROM
(select a.*,s.msisdn as msisdn_s
from
(SELECT raison_sociale AS NOM_STRUCTURE, msisdn
    ,NUMERO_REGISTRE_COMMERCE,cni_representant_local AS NUMERO_PIECE_REPRESENTANT_LEGAL
    FROM DIM.SPARK_DT_BDI_STK_PERS_MORALE
    where not(msisdn is null or trim(msisdn) = '')
) a
JOIN
(SELECT * FROM DIM.SPARK_DT_BDI_STK_LIGNE_FLOTTE where not(MSISDN_PARENT is null or trim(MSISDN_PARENT) = '')) s
ON  FN_FORMAT_MSISDN_TO_9DIGITS(trim(a.msisdn)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(s.MSISDN_PARENT))
) c
JOIN
(SELECT * FROM TMP.TT_bdi where not(msisdn is null or trim(msisdn) = '')) b ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(c.MSISDN_s)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(b.MSISDN))