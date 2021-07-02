insert into TMP.TT_BDI_FLOTTE_1A 
select 
trim(MSISDN) AS MSISDN,
trim(CUSTOMER_ID) AS CUSTOMER_ID,
trim(CONTRACT_ID) AS CONTRACT_ID,
trim(COMPTE_CLIENT) AS COMPTE_CLIENT,
trim(TYPE_PERSONNE) AS TYPE_PERSONNE,
trim(TYPE_PIECE) AS TYPE_PIECE,
trim(NUMERO_PIECE) AS NUMERO_PIECE,
trim(ID_TYPE_PIECE) AS ID_TYPE_PIECE,
trim(NOM_PRENOM) AS NOM_PRENOM,
trim(NOM) AS NOM,
trim(PRENOM) AS PRENOM,
trim(DATE_NAISSANCE) AS DATE_NAISSANCE,
trim(DATE_EXPIRATION) AS DATE_EXPIRATION,
trim(ADRESSE) AS ADRESSE,
trim(VILLE) AS VILLE,
trim(QUARTIER) AS QUARTIER,
trim(DATE_SOUSCRIPTION) AS DATE_SOUSCRIPTION,
trim(DATE_ACTIVATION) AS DATE_ACTIVATION,
trim(STATUT) AS STATUT,
trim(RAISON_STATUT) AS RAISON_STATUT,
trim(DATE_CHANGEMENT_STATUT) AS DATE_CHANGEMENT_STATUT,
trim(PLAN_LOCALISATION) AS PLAN_LOCALISATION,
trim(CONTRAT_SOUCRIPTION) AS CONTRAT_SOUCRIPTION,
trim(DISPONIBILITE_SCAN) AS DISPONIBILITE_SCAN,
trim(ACCEPTATION_CGV) AS ACCEPTATION_CGV,
trim(TYPE_PIECE_TUTEUR) AS TYPE_PIECE_TUTEUR,
trim(NUMERO_PIECE_TUTEUR) AS NUMERO_PIECE_TUTEUR,
trim(NOM_TUTEUR) AS NOM_TUTEUR,
trim(PRENOM_TUTEUR) AS PRENOM_TUTEUR,
trim(DATE_NAISSANCE_TUTEUR) AS DATE_NAISSANCE_TUTEUR,
trim(DATE_EXPIRATION_TUTEUR) AS DATE_EXPIRATION_TUTEUR,
trim(ADRESSE_TUTEUR) AS ADRESSE_TUTEUR,
trim(COMPTE_CLIENT_STRUCTURE) AS COMPTE_CLIENT_STRUCTURE,
trim(NOM_STRUCTURE) AS NOM_STRUCTURE,
trim(NUMERO_REGISTRE_COMMERCE) AS NUMERO_REGISTRE_COMMERCE,
trim(NUMERO_PIECE_REPRESENTANT_LEGAL) AS NUMERO_PIECE_REPRESENTANT_LEGAL,
trim(IMEI) AS IMEI,
trim(STATUT_DEROGATION) AS STATUT_DEROGATION,
trim(REGION_ADMINISTRATIVE) AS REGION_ADMINISTRATIVE,
trim(REGION_COMMERCIALE) AS REGION_COMMERCIALE,
trim(SITE_NAME) AS SITE_NAME,
trim(VILLE_SITE) AS VILLE_SITE,
trim(OFFRE_COMMERCIALE) AS OFFRE_COMMERCIALE,
trim(TYPE_CONTRAT) AS TYPE_CONTRAT,
trim(SEGMENTATION) AS SEGMENTATION,
trim(odbIncomingCalls) AS odbIncomingCalls,
trim(odbOutgoingCalls) AS odbOutgoingCalls,
trim(DEROGATION_IDENTIFICATION) AS DEROGATION_IDENTIFICATION
from (
select
a.MSISDN MSISDN,
a.CUSTOMER_ID CUSTOMER_ID,
a.CONTRACT_ID CONTRACT_ID,
a.COMPTE_CLIENT COMPTE_CLIENT,
a.TYPE_PERSONNE TYPE_PERSONNE,
a.TYPE_PIECE TYPE_PIECE,
a.NUMERO_PIECE NUMERO_PIECE,
a.ID_TYPE_PIECE ID_TYPE_PIECE,
a.NOM_PRENOM NOM_PRENOM,
a.NOM NOM,
a.PRENOM PRENOM,
a.DATE_NAISSANCE DATE_NAISSANCE,
a.DATE_EXPIRATION DATE_EXPIRATION,
a.ADRESSE ADRESSE,
a.VILLE VILLE,
a.QUARTIER QUARTIER,
a.DATE_SOUSCRIPTION DATE_SOUSCRIPTION,
a.DATE_ACTIVATION DATE_ACTIVATION,
a.STATUT STATUT,
a.RAISON_STATUT RAISON_STATUT,
a.DATE_CHANGEMENT_STATUT DATE_CHANGEMENT_STATUT,
a.PLAN_LOCALISATION PLAN_LOCALISATION,
a.CONTRAT_SOUCRIPTION CONTRAT_SOUCRIPTION,
a.DISPONIBILITE_SCAN DISPONIBILITE_SCAN,
a.ACCEPTATION_CGV ACCEPTATION_CGV,
a.TYPE_PIECE_TUTEUR TYPE_PIECE_TUTEUR,
a.NUMERO_PIECE_TUTEUR NUMERO_PIECE_TUTEUR,
a.NOM_TUTEUR NOM_TUTEUR,
a.PRENOM_TUTEUR PRENOM_TUTEUR,
a.DATE_NAISSANCE_TUTEUR DATE_NAISSANCE_TUTEUR,
a.DATE_EXPIRATION_TUTEUR DATE_EXPIRATION_TUTEUR,
a.ADRESSE_TUTEUR ADRESSE_TUTEUR,
a.COMPTE_CLIENT_STRUCTURE COMPTE_CLIENT_STRUCTURE,
a.NOM_STRUCTURE NOM_STRUCTURE,
a.NUMERO_REGISTRE_COMMERCE NUMERO_REGISTRE_COMMERCE,
a.NUMERO_PIECE_REPRESENTANT_LEGAL NUMERO_PIECE_REPRESENTANT_LEGAL,
a.IMEI IMEI,
a.STATUT_DEROGATION STATUT_DEROGATION,
a.REGION_ADMINISTRATIVE REGION_ADMINISTRATIVE,
a.REGION_COMMERCIALE REGION_COMMERCIALE,
a.SITE_NAME SITE_NAME,
a.VILLE_SITE VILLE_SITE,
a.OFFRE_COMMERCIALE OFFRE_COMMERCIALE,
a.TYPE_CONTRAT TYPE_CONTRAT,
a.SEGMENTATION SEGMENTATION,
a.odbIncomingCalls odbIncomingCalls,
a.odbOutgoingCalls odbOutgoingCalls,
'N' AS DEROGATION_IDENTIFICATION,
b.compte_client as compte_client_b
from  (select * from TMP.TT_bdi_1A
 where trim(compte_client_structure) like '4.%') a
, (select distinct compte_client from TMP.TT_BDI_PERS_MORALE_TMP_1A where not(compte_client is null or trim(compte_client) = '')) b
) x
where substr(trim(compte_client_structure),1,6) = substr(trim(compte_client_b),1,6)