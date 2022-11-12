-- Etape 1 : Extraction des toutes les Multsims et des lignes ayant des champs en anomalie, puis reclassement de ses lignes en M2M Génériques(M2MG)
insert into TMP.TT_KYC_BDI_FLOTTE_AM_ST1
select
'GENERIC_GUID' CUST_GUID,
a.MSISDN  MSISDN,
b.CUSTOMER_ID  CUSTOMER_ID,
b.CONTRACT_ID  CONTRACT_ID,
'4.4335'  COMPTE_CLIENT,
'M2MG'  TYPE_PERSONNE,
null  TYPE_PIECE,
null  NUMERO_PIECE,
null  ID_TYPE_PIECE,
null  NOM_PRENOM,
null  NOM,
null  PRENOM,
null  DATE_NAISSANCE,
null  DATE_EXPIRATION,
null  ADRESSE,
'DOUALA'  VILLE,
'AKWA'  QUARTIER,
b.DATE_SOUSCRIPTION  DATE_SOUSCRIPTION,
b.DATE_ACTIVATION  DATE_ACTIVATION,
b.statut  STATUT,
b.RAISON_STATUT  RAISON_STATUT,
b.DATE_CHANGEMENT_STATUT  DATE_CHANGEMENT_STATUT,
null  PLAN_LOCALISATION,
null  CONTRAT_SOUCRIPTION,
null  DISPONIBILITE_SCAN,
null  ACCEPTATION_CGV,
null  TYPE_PIECE_TUTEUR,
null  NUMERO_PIECE_TUTEUR,
null  NOM_TUTEUR,
null  PRENOM_TUTEUR,
null  DATE_NAISSANCE_TUTEUR,
null  DATE_EXPIRATION_TUTEUR,
null  ADRESSE_TUTEUR,
'4.4335'  COMPTE_CLIENT_STRUCTURE,
'FORIS TELECOM'  NOM_STRUCTURE,
'RC/DLA/2008/B/782' NUMERO_REGISTRE_COMMERCE,
'110046646' NUMERO_PIECE_REPRESENTANT_LEGAL,
'DOUALA CM AKWA'  ADRESSE_STRUCTURE,
b.IMEI  IMEI,
null  STATUT_DEROGATION,
b.REGION_ADMINISTRATIVE  REGION_ADMINISTRATIVE,
b.REGION_COMMERCIALE  REGION_COMMERCIALE,
b.SITE_NAME  SITE_NAME,
b.VILLE_SITE  VILLE_SITE,
b.OFFRE_COMMERCIALE  OFFRE_COMMERCIALE,
b.TYPE_CONTRAT  TYPE_CONTRAT,
b.SEGMENTATION  SEGMENTATION,
b.odbIncomingCalls  odbIncomingCalls,
b.odbOutgoingCalls  odbOutgoingCalls,
null  DEROGATION_IDENTIFICATION
from (select * from MON.SPARK_FT_KYC_BDI_PP where event_date=to_date('###SLICE_VALUE###')
and (((DATE_ACTIVATION IS NULL OR NOM_PRENOM_ABSENT = 'OUI'
    OR NOM_PRENOM_DOUTEUX = 'OUI' OR NUMERO_PIECE_ABSENT = 'OUI'
    OR NUMERO_PIECE_INF_4 = 'OUI' OR NUMERO_PIECE_NON_AUTHORISE = 'OUI'
    OR NUMERO_PIECE_EGALE_MSISDN = 'OUI' OR NUMERO_PIECE_A_CARACT_NON_AUTH = 'OUI'
    OR NUMERO_PIECE_UNIQUEMENT_LETTRE = 'OUI' OR DATE_EXPIRATION_DOUTEUSE = 'OUI'
    OR DATE_EXPIRATION is null OR DATE_NAISSANCE is null
    OR DATE_NAISSANCE_DOUTEUX = 'OUI'
    OR ADRESSE_ABSENT = 'OUI' OR ADRESSE_DOUTEUSE = 'OUI'
    OR IMEI is null OR trim(IMEI) = '' OR TYPE_PIECE IS NULL OR trim(TYPE_PIECE) = ''
    OR MSISDN IS NULL OR trim(MSISDN) = '')
AND type_personne IN ('MAJEUR','PP','PDV')) OR
((DATE_ACTIVATION IS NULL OR NOM_PRENOM_ABSENT = 'OUI'
    OR NOM_PRENOM_DOUTEUX = 'OUI' OR NUMERO_PIECE_ABSENT = 'OUI'
    OR NUMERO_PIECE_INF_4 = 'OUI' OR NUMERO_PIECE_NON_AUTHORISE = 'OUI'
    OR NUMERO_PIECE_EGALE_MSISDN = 'OUI' OR NUMERO_PIECE_A_CARACT_NON_AUTH = 'OUI'
    OR NUMERO_PIECE_UNIQUEMENT_LETTRE = 'OUI' OR DATE_EXPIRATION_DOUTEUSE = 'OUI'
    OR DATE_EXPIRATION is null OR TYPE_PIECE IS NULL OR trim(TYPE_PIECE) = ''
    OR DATE_NAISSANCE is null OR DATE_NAISSANCE_DOUTEUX = 'OUI'
    OR NOM_PARENT_ABSENT = 'OUI' OR NOM_PARENT_DOUTEUX = 'OUI'
    OR NUMERO_PIECE_TUT_ABSENT = 'OUI' OR NUMERO_PIECE_TUT_INF_4 = 'OUI'
    OR NUMERO_PIECE_TUT_NON_AUTH = 'OUI' OR NUMERO_PIECE_TUT_EGALE_MSISDN = 'OUI'
    OR NUMERO_PIECE_TUT_CARAC_NON_A = 'OUI' OR NUMERO_PIECE_TUT_UNIQ_LETTRE = 'OUI'
    OR date_naissance_tuteur is null OR DATE_NAISSANCE_TUT_DOUTEUX ='OUI'
    OR ADRESSE_ABSENT = 'OUI' OR ADRESSE_DOUTEUSE = 'OUI' OR IMEI is null OR trim(IMEI) = ''
    OR MSISDN IS NULL OR trim(MSISDN) = '') AND type_personne IN ('MINEUR','PDV')))) A
left join (select * from CDR.SPARK_IT_KYC_BDI_FULL where original_file_date=date_add(to_date('###SLICE_VALUE###'),1)) B on A.MSISDN = B.MSISDN