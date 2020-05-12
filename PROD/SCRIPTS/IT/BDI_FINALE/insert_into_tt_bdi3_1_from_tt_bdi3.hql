insert into TMP.tt_bdi3_1
select
trim(B.MSISDN) AS MSISDN,
trim(B.TYPE_PERSONNE) AS TYPE_PERSONNE,
trim(B.NOM_PRENOM) AS NOM_PRENOM,
trim(B.ID_TYPE_PIECE) AS ID_TYPE_PIECE,
trim(B.TYPE_PIECE) AS TYPE_PIECE,
trim(B.NUMERO_PIECE) AS NUMERO_PIECE,
trim(B.DATE_EXPIRATION) AS DATE_EXPIRATION,
trim(B.DATE_NAISSANCE) AS DATE_NAISSANCE,
trim(B.DATE_ACTIVATION) AS DATE_ACTIVATION,
trim(B.ADRESSE) AS ADRESSE,
trim(B.QUARTIER) AS QUARTIER,
trim(B.VILLE) AS VILLE,
trim(B.STATUT) AS STATUT,
trim(B.STATUT_VALIDATION_BO) AS STATUT_VALIDATION_BO,
trim(B.MOTIF_REJET_BO) AS MOTIF_REJET_BO,
trim(B.DATE_VALIDATION_BO) AS DATE_VALIDATION_BO,
trim(B.LOGIN_VALIDATEUR_BO) AS LOGIN_VALIDATEUR_BO,
trim(B.CANAL_VALIDATEUR_BO) AS CANAL_VALIDATEUR_BO,
trim(B.TYPE_ABONNEMENT) AS TYPE_ABONNEMENT,
trim(B.CSMODDATE) AS CSMODDATE,
trim(B.CCMODDATE) AS CCMODDATE,
trim(B.COMPTE_CLIENT_STRUCTURE) AS COMPTE_CLIENT_STRUCTURE,
trim(B.NOM_STRUCTURE) AS NOM_STRUCTURE,
trim(B.NUMERO_REGISTRE_COMMERCE) AS NUMERO_REGISTRE_COMMERCE,
trim(B.NUMERO_PIECE_REPRESENTANT_LEGAL) AS NUMERO_PIECE_REPRESENTANT_LEGAL,
trim(B.IMEI) AS IMEI,
trim(B.STATUT_DEROGATION) AS STATUT_DEROGATION,
trim(B.REGION_ADMINISTRATIVE) AS REGION_ADMINISTRATIVE,
trim(B.REGION_COMMERCIALE) AS REGION_COMMERCIALE,
trim(B.SITE_NAME) AS SITE_NAME,
trim(B.VILLE_SITE) AS VILLE_SITE,
trim(B.OFFRE_COMMERCIALE) AS OFFRE_COMMERCIALE,
trim(B.TYPE_CONTRAT) AS TYPE_CONTRAT,
trim(B.SEGMENTATION) AS SEGMENTATION,
trim(B.SCORE_VIP) AS SCORE_VIP,
trim(B.DATE_SOUSCRIPTION) AS DATE_SOUSCRIPTION,
trim(B.DATE_CHANGEMENT_STATUT) AS DATE_CHANGEMENT_STATUT,
trim(B.VILLE_STRUCTURE) AS VILLE_STRUCTURE,
trim(B.QUARTIER_STRUCTURE) AS QUARTIER_STRUCTURE,
trim(B.RAISON_STATUT) AS RAISON_STATUT,
trim(B.PRENOM) AS PRENOM,
trim(B.NOM) AS NOM,
trim(B.CUSTOMER_ID) AS CUSTOMER_ID,
trim(B.CONTRACT_ID) AS CONTRACT_ID,
trim(B.COMPTE_CLIENT) AS COMPTE_CLIENT,
trim(B.PLAN_LOCALISATION) AS PLAN_LOCALISATION,
trim(B.CONTRAT_SOUCRIPTION) AS CONTRAT_SOUCRIPTION,
trim(B.ACCEPTATION_CGV) AS ACCEPTATION_CGV,
trim(B.DISPONIBILITE_SCAN) AS DISPONIBILITE_SCAN,
trim(B.NOM_TUTEUR) AS NOM_TUTEUR,
trim(B.PRENOM_TUTEUR) AS PRENOM_TUTEUR,
trim(B.DATE_NAISSANCE_TUTEUR) AS DATE_NAISSANCE_TUTEUR,
trim(B.NUMERO_PIECE_TUTEUR) AS NUMERO_PIECE_TUTEUR,
trim(B.DATE_EXPIRATION_TUTEUR) AS DATE_EXPIRATION_TUTEUR,
trim(B.ID_TYPE_PIECE_TUTEUR) AS ID_TYPE_PIECE_TUTEUR,
trim(B.TYPE_PIECE_TUTEUR) AS TYPE_PIECE_TUTEUR,
trim(B.ADRESSE_TUTEUR) AS ADRESSE_TUTEUR,
trim(B.IDENTIFICATEUR) AS IDENTIFICATEUR,
trim(B.LOCALISATION_IDENTIFICATEUR) AS LOCALISATION_IDENTIFICATEUR,
trim(B.PROFESSION) AS PROFESSION,
trim(B.odbIncomingCalls) AS odbIncomingCalls,
trim(B.odbOutgoingCalls) AS odbOutgoingCalls
from (
select a2.*,
row_number() over(partition by a2.msisdn order by a2.date_activation2 DESC NULLS LAST) AS RANG
from (
select a.*,
(CASE
WHEN trim(a.DATE_ACTIVATION) IS NULL OR trim(a.DATE_ACTIVATION) = '' THEN NULL
WHEN trim(a.DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(a.DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(a.DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION2
from  TMP.TT_BDI3 a
) a2
) B where RANG = 1