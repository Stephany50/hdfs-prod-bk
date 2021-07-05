INSERT INTO TMP.TT_KYC_PERS_PHYSIQUE_B2C_FLOTTE
SELECT
t_bdi.MSISDN MSISDN, 
t_bdi.TYPE_PERSONNE TYPE_PERSONNE, 
IF(t_bdi.NOM_PRENOM is null or trim(t_bdi.NOM_PRENOM) = '',trim(zm.nom_prenom),trim(t_bdi.NOM_PRENOM)) NOM_PRENOM, 
IF(t_bdi.ID_TYPE_PIECE is null or trim(t_bdi.ID_TYPE_PIECE) = '',trim(zm.ID_TYPE_PIECE),trim(t_bdi.ID_TYPE_PIECE)) ID_TYPE_PIECE, 
t_bdi.TYPE_PIECE TYPE_PIECE, 
IF(t_bdi.NUMERO_PIECE is null or trim(t_bdi.NUMERO_PIECE) = '',trim(zm.NUMERO_PIECE),trim(t_bdi.NUMERO_PIECE)) NUMERO_PIECE, 
IF(t_bdi.DATE_EXPIRATION is null or trim(t_bdi.DATE_EXPIRATION) = '',trim(zm.DATE_EXPIRATION),trim(t_bdi.DATE_EXPIRATION)) AS  DATE_EXPIRATION, 
IF(t_bdi.DATE_NAISSANCE is null or trim(t_bdi.DATE_NAISSANCE) = '',trim(zm.DATE_NAISSANCE),trim(t_bdi.DATE_NAISSANCE)) AS   DATE_NAISSANCE, 
IF(zm.DATE_ACTIVATION is null or trim(zm.DATE_ACTIVATION) = '',trim(t_bdi.DATE_ACTIVATION),trim(zm.DATE_ACTIVATION)) DATE_ACTIVATION, 
IF(t_bdi.ADRESSE is null or trim(t_bdi.ADRESSE) = '',trim(zm.ADRESSE),trim(t_bdi.ADRESSE)) AS ADRESSE, 
IF(t_bdi.QUARTIER is null or trim(t_bdi.QUARTIER) = '',trim(zm.QUARTIER),trim(t_bdi.QUARTIER)) AS QUARTIER, 
IF(t_bdi.VILLE is null or trim(t_bdi.VILLE) = '', trim(zm.VILLE),trim(t_bdi.VILLE)) VILLE, 
trim(zm.STATUT) AS STATUT, 
IF(t_bdi.STATUT_VALIDATION_BO is null or trim(t_bdi.STATUT_VALIDATION_BO) = '',trim(zm.STATUT_VALIDATION_BO),trim(t_bdi.STATUT_VALIDATION_BO)) AS STATUT_VALIDATION_BO, 
IF(t_bdi.MOTIF_REJET_BO is null or trim(t_bdi.MOTIF_REJET_BO) = '',trim(zm.MOTIF_REJET_BO),trim(t_bdi.MOTIF_REJET_BO)) AS MOTIF_REJET_BO, 
IF(t_bdi.DATE_VALIDATION_BO is null or trim(t_bdi.DATE_VALIDATION_BO) = '',trim(zm.DATE_VALIDATION_BO),trim(t_bdi.DATE_VALIDATION_BO)) DATE_VALIDATION_BO, 
IF(t_bdi.LOGIN_VALIDATEUR_BO is null or trim(t_bdi.LOGIN_VALIDATEUR_BO) = '',trim(zm.LOGIN_VALIDATEUR_BO),trim(t_bdi.LOGIN_VALIDATEUR_BO)) LOGIN_VALIDATEUR_BO, 
IF(t_bdi.CANAL_VALIDATEUR_BO is null or trim(t_bdi.CANAL_VALIDATEUR_BO) = '',trim(zm.CANAL_VALIDATEUR_BO),trim(t_bdi.CANAL_VALIDATEUR_BO)) CANAL_VALIDATEUR_BO, 
IF(t_bdi.TYPE_ABONNEMENT is null OR trim(t_bdi.TYPE_ABONNEMENT) = '',trim(zm.TYPE_ABONNEMENT),trim(t_bdi.TYPE_ABONNEMENT)) TYPE_ABONNEMENT, 
zm.CSMODDATE CSMODDATE, 
zm.CCMODDATE CCMODDATE, 
IF(t_bdi.COMPTE_CLIENT_STRUCTURE is null or trim(t_bdi.COMPTE_CLIENT_STRUCTURE) = '',trim(zm.COMPTE_CLIENT_STRUCTURE),trim(t_bdi.COMPTE_CLIENT_STRUCTURE)) COMPTE_CLIENT_STRUCTURE, 
IF(t_bdi.NOM_STRUCTURE is null or trim(t_bdi.NOM_STRUCTURE) = '',trim(zm.NOM_STRUCTURE),trim(t_bdi.NOM_STRUCTURE)) NOM_STRUCTURE, 
IF(t_bdi.NUMERO_REGISTRE_COMMERCE is null or trim(t_bdi.NUMERO_REGISTRE_COMMERCE) = '',trim(zm.NUMERO_REGISTRE_COMMERCE),trim(t_bdi.NUMERO_REGISTRE_COMMERCE)) NUMERO_REGISTRE_COMMERCE, 
IF(t_bdi.NUMERO_PIECE_REPRESENTANT_LEGAL is null or trim(t_bdi.NUMERO_PIECE_REPRESENTANT_LEGAL) = '',trim(zm.NUMERO_PIECE_REPRESENTANT_LEGAL),trim(t_bdi.NUMERO_PIECE_REPRESENTANT_LEGAL)) NUMERO_PIECE_REPRESENTANT_LEGAL,
IF( t_bdi.IMEI = '' OR t_bdi.IMEI IS NULL, '351000010000100',t_bdi.IMEI) IMEI, 
t_bdi.STATUT_DEROGATION STATUT_DEROGATION,
t_bdi.REGION_ADMINISTRATIVE REGION_ADMINISTRATIVE,
t_bdi.REGION_COMMERCIALE REGION_COMMERCIALE,
t_bdi.SITE_NAME SITE_NAME,
t_bdi.VILLE_SITE VILLE_SITE,
t_bdi.OFFRE_COMMERCIALE OFFRE_COMMERCIALE,
t_bdi.TYPE_CONTRAT TYPE_CONTRAT,
t_bdi.SEGMENTATION SEGMENTATION,
null as SCORE_VIP,
nvl(trim(zm.DATE_SOUSCRIPTION),trim(t_bdi.DATE_SOUSCRIPTION)) DATE_SOUSCRIPTION, 
nvl(trim(zm.DATE_CHANGEMENT_STATUT),trim(t_bdi.DATE_CHANGEMENT_STATUT)) DATE_CHANGEMENT_STATUT , 
nvl(trim(t_bdi.VILLE_STRUCTURE),trim(zm.VILLE_STRUCTURE)) VILLE_STRUCTURE, 
nvl(trim(t_bdi.QUARTIER_STRUCTURE),trim(zm.QUARTIER_STRUCTURE)) QUARTIER_STRUCTURE, 
nvl(trim(zm.RAISON_STATUT),trim(t_bdi.RAISON_STATUT)) RAISON_STATUT, 
nvl(trim(t_bdi.PRENOM),trim(zm.PRENOM)) PRENOM, 
nvl(trim(t_bdi.NOM),trim(zm.NOM)) NOM, 
nvl(trim(t_bdi.CUSTOMER_ID),trim(zm.CUSTOMER_ID)) CUSTOMER_ID, 
nvl(trim(t_bdi.CONTRACT_ID),trim(zm.CONTRACT_ID)) CONTRACT_ID, 
nvl(trim(t_bdi.COMPTE_CLIENT),trim(zm.COMPTE_CLIENT)) COMPTE_CLIENT, 
nvl(trim(t_bdi.PLAN_LOCALISATION),trim(zm.PLAN_LOCALISATION)) PLAN_LOCALISATION, 
nvl(trim(t_bdi.CONTRAT_SOUCRIPTION),trim(zm.CONTRAT_SOUCRIPTION)) CONTRAT_SOUCRIPTION, 
nvl(trim(t_bdi.ACCEPTATION_CGV),trim(zm.ACCEPTATION_CGV)) ACCEPTATION_CGV, 
nvl(trim(t_bdi.DISPONIBILITE_SCAN),trim(zm.DISPONIBILITE_SCAN)) DISPONIBILITE_SCAN, 
nvl(trim(t_bdi.NOM_TUTEUR),trim(zm.NOM_TUTEUR)) NOM_TUTEUR, 
nvl(trim(t_bdi.PRENOM_TUTEUR),trim(zm.PRENOM_TUTEUR)) PRENOM_TUTEUR, 
nvl(trim(t_bdi.DATE_NAISSANCE_TUTEUR),trim(zm.DATE_NAISSANCE_TUTEUR)) DATE_NAISSANCE_TUTEUR, 
nvl(trim(t_bdi.NUMERO_PIECE_TUTEUR),trim(zm.NUMERO_PIECE_TUTEUR)) NUMERO_PIECE_TUTEUR, 
nvl(trim(t_bdi.DATE_EXPIRATION_TUTEUR),trim(zm.DATE_EXPIRATION_TUTEUR)) DATE_EXPIRATION_TUTEUR, 
nvl(trim(t_bdi.ID_TYPE_PIECE_TUTEUR),trim(zm.ID_TYPE_PIECE_TUTEUR)) ID_TYPE_PIECE_TUTEUR, 
t_bdi.TYPE_PIECE_TUTEUR TYPE_PIECE_TUTEUR, 
nvl(trim(t_bdi.ADRESSE_TUTEUR),trim(zm.ADRESSE_TUTEUR)) ADRESSE_TUTEUR, 
nvl(trim(t_bdi.IDENTIFICATEUR),trim(zm.IDENTIFICATEUR)) IDENTIFICATEUR, 
nvl(trim(t_bdi.LOCALISATION_IDENTIFICATEUR),trim(zm.LOCALISATION_IDENTIFICATEUR)) LOCALISATION_IDENTIFICATEUR, 
nvl(trim(t_bdi.PROFESSION),trim(zm.PROFESSION)) PROFESSION, 
hlr.odbIncomingCalls odbIncomingCalls, 
hlr.odbOutgoingCalls odbOutgoingCalls 
FROM (SELECT * FROM TMP.TT_KYC_PERS_PHY_B2C_FLOTTE_LOC where not(msisdn is null or trim(msisdn) = '')) t_bdi
LEFT JOIN  (select a.*,
nvl(trim(a.nom),'') || ' ' || nvl(trim(a.prenom),'') AS nom_prenom
from CDR.SPARK_IT_BDI_ZSMART a where original_file_date = '###SLICE_VALUE###'
 and not(msisdn is null or trim(msisdn) = '')) zm
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(t_bdi.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(zm.MSISDN))
LEFT JOIN (select
trim(msisdn) AS msisdn,
trim(odbic) AS odbincomingcalls,
trim(odboc) AS odboutgoingcalls,
trim(imsi) AS profileid
from CDR.SPARK_IT_BDI_HLR where original_file_date = '###SLICE_VALUE###'
 and not(msisdn is null or trim(msisdn) = '')) hlr
on FN_FORMAT_MSISDN_TO_9DIGITS(trim(t_bdi.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(hlr.MSISDN))