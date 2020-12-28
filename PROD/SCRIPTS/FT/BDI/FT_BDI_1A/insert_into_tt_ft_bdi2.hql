insert into TMP.TT_FT_BDI_1A
SELECT
FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) AS MSISDN,
trim(A.TYPE_PIECE) AS TYPE_PIECE,
trim(A.NUMERO_PIECE) AS NUMERO_PIECE,
UPPER(trim(A.NOM_PRENOM)) AS NOM_PRENOM,
UPPER(trim(A.NOM)) AS NOM,
UPPER(trim(A.PRENOM)) AS PRENOM,
DATE_NAISSANCE,
DATE_EXPIRATION,
UPPER(trim(A.ADDRESSE)) AS ADRESSE,
trim(A.NUMERO_PIECE_TUTEUR) AS NUMERO_PIECE_TUTEUR,
trim(A.NOM_PARENT) AS NOM_PARENT,
DATE_NAISSANCE_TUTEUR,
trim(A.NOM_STRUCTURE) AS NOM_STRUCTURE,
trim(A.NUMERO_REGISTRE_COMMERCE) AS NUMERO_REGISTRE_COMMERCE,
trim(A.NUMERO_PIECE_REP_LEGAL) AS NUMERO_PIECE_REP_LEGAL,
DATE_ACTIVATION,
DATE_CHANGEMENT_STATUT,
UPPER(trim(A.STATUT_BSCS)) AS STATUT_BSCS,
trim(A.ODBINCOMINGCALLS) AS ODBINCOMINGCALLS,
trim(A.ODBOUTGOINGCALLS) AS ODBOUTGOINGCALLS,
trim(A.IMEI) AS IMEI,
(CASE WHEN F.MSISDN IS NULL OR trim(F.MSISDN) = '' THEN 'NON' ELSE 'OUI' END) STATUT_DEROGATION,
trim(B.ADMINISTRATIVE_REGION) AS REGION_ADMINISTRATIVE,
trim(B.COMMERCIAL_REGION) AS REGION_COMMERCIALE,
trim(B.SITE_NAME) AS SITE_NAME,
trim(B.TOWNNAME) AS VILLE,
trim(B.LONGITUDE) AS LONGITUDE,
trim(B.LATITUDE) AS LATITUDE,
(Case when C.access_key is not null AND trim(C.access_key) <> ''
THEN trim(C.COMMERCIAL_OFFER)  ELSE 'N/A'
END) OFFRE_COMMERCIALE,
( Case when trim(C.access_key) is not null AND trim(C.access_key) <> ''
THEN UPPER(NVL(C.OSP_CONTRACT_TYPE, C.OSP_ACCOUNT_TYPE)) ELSE 'N/A'
END) TYPE_CONTRAT,
(Case when trim(C.access_key) is not null AND trim(C.access_key) <> ''
THEN trim(C.SEGMENTATION) ELSE 'N/A'
END) SEGMENTATION,
0  REV_M_3,
0  REV_M_2,
0 REV_M_1,
0 REV_MOY,
(Case when trim(C.access_key) is not null AND trim(C.access_key) <> ''
THEN trim(C.OSP_STATUS)
ELSE 'N/A' END) AS STATUT_IN,
(CASE WHEN trim(A.NUMERO_PIECE) = '' OR A.NUMERO_PIECE IS NULL THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_ABSENT,
(CASE WHEN trim(A.NUMERO_PIECE_TUTEUR) = '' OR A.NUMERO_PIECE_TUTEUR IS NULL THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_TUT_ABSENT,
(CASE WHEN not(trim(A.NUMERO_PIECE) is null or trim(A.NUMERO_PIECE) = '') AND LENGTH(trim(A.NUMERO_PIECE)) < 4
THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_INF_4,
(CASE WHEN not(trim(A.NUMERO_PIECE_TUTEUR) is null or trim(A.NUMERO_PIECE_TUTEUR) = '') AND
LENGTH(trim(A.NUMERO_PIECE_TUTEUR)) < 4 THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_TUT_INF_4,
(
CASE WHEN TRIM(A.NUMERO_PIECE) IN ('000000000', '111111111', '222222222', '333333333',
'444444444', '555555555', '666666666', '777777777', '888888888', '999999999',
'012345678','122222222', '123456789', '100010001', '1122334455') OR
LENGTH(trim(A.NUMERO_PIECE)) > 21 OR TRIM(A.NUMERO_PIECE) LIKE '112233445%' THEN 'OUI' ELSE 'NON' END
) NUMERO_PIECE_NON_AUTHORISE,
(
CASE WHEN TRIM(A.NUMERO_PIECE_TUTEUR) IN ('000000000', '111111111', '222222222', '333333333',
'444444444', '555555555', '666666666', '777777777', '888888888', '999999999',
'012345678','122222222', '123456789', '100010001', '1122334455') OR LENGTH(TRIM(A.NUMERO_PIECE_TUTEUR)) > 21 OR
trim(A.NUMERO_PIECE) = trim(A.NUMERO_PIECE_TUTEUR) OR TRIM(A.NUMERO_PIECE_TUTEUR) LIKE '112233445%' THEN 'OUI' ELSE 'NON' END
) NUMERO_PIECE_TUT_NON_AUTH,
(CASE WHEN trim(A.MSISDN) = trim(A.NUMERO_PIECE) THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_EGALE_MSISDN,
(CASE WHEN trim(A.MSISDN) = trim(A.NUMERO_PIECE_TUTEUR) THEN 'OUI' ELSE 'NON' END) NUMERO_PIECE_TUT_EGALE_MSISDN,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE)), 'abcdefghijklmnopqMCMLXXXIVrstuvwxyz1234567890-/',' ')) <> ''
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_A_CARACT_NON_AUTH,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE_TUTEUR)), 'abcdefghijklmnopMCMLXXXIVqrstuvwxyz1234567890-/',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE_TUTEUR)), 'abcdefghijklmnopMCMLXXXIVqrstuvwxyz1234567890-/',' ')) <> ''
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_TUT_CARAC_NON_A,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE)), 'abcdefghijklmnopqrMCMLXXXIVstuvwxyz-',' ')) is null
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_UNIQUEMENT_LETTRE,
(CASE WHEN TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE_TUTEUR)), 'abcdefghijklMCMLXXXIVmnopqrstuvwxyz-',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(A.NUMERO_PIECE_TUTEUR)), 'abcdefghijklMCMLXXXIVmnopqrstuvwxyz-',' ')) is null
THEN 'OUI' ELSE 'NON'
END) NUMERO_PIECE_TUT_UNIQ_LETTRE,
(CASE WHEN trim(A.NOM_PRENOM) = '' OR A.NOM_PRENOM IS NULL THEN 'OUI' ELSE 'NON' END) NOM_PRENOM_ABSENT,
(CASE WHEN trim(A.NOM_PARENT) = '' OR A.NOM_PARENT IS NULL THEN 'OUI' ELSE 'NON' END) NOM_PARENT_ABSENT,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(A.NOM_PRENOM)),'aeiou',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(A.NOM_PRENOM)),'aeiou',' ')) is null)
OR (TRIM(TRANSLATE(LOWER(trim(A.NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' '))  = ''
OR TRIM(TRANSLATE(LOWER(trim(A.NOM_PRENOM)),'bcdfghjklmnpqrstvwxz',' ')) is null)
OR (TRIM(TRANSLATE(trim(A.NOM_PRENOM),'1234567890.',' '))  = ''
OR TRIM(TRANSLATE(trim(A.NOM_PRENOM),'1234567890.',' ')) is null)
OR (LENGTH(trim(A.NOM_PRENOM)) <= 1 and trim(A.NOM_PRENOM) is not null and trim(A.NOM_PRENOM) <> '')
OR LOWER(TRIM(A.NOM_PRENOM)) IN ('orange', 'vendeur', 'madame', 'monsieur', 'delta', 'phone', 'inconnu', 'inconnue', 'anonyme', 'unknown')
OR (TRIM(TRANSLATE(LOWER(trim(A.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöî\'ôûâê-.',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(A.NOM_PRENOM)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxcvbnm1MCMLXXXIV234567890¿çéèàäëüïöîôû\'âê-.',' ')) <> ''
)
THEN 'OUI' ELSE 'NON'
END) NOM_PRENOM_DOUTEUX,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(A.NOM_PARENT)),'aeiou',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(A.NOM_PARENT)),'aeiou',' ')) is null)
OR (TRIM(TRANSLATE(LOWER(trim(A.NOM_PARENT)),'bcdfghjklmnpqrstvwxz',' ')) = ''
OR TRIM(TRANSLATE(LOWER(trim(A.NOM_PARENT)),'bcdfghjklmnpqrstvwxz',' ')) is null
)
OR (TRIM(TRANSLATE(trim(A.NOM_PARENT),'1234567890.',' ')) = ''
OR TRIM(TRANSLATE(trim(A.NOM_PARENT),'1234567890.',' ')) is null)
OR (LENGTH(trim(A.NOM_PARENT)) <= 1 and trim(A.NOM_PARENT) is not null and trim(A.NOM_PARENT) <> '')
OR LOWER(TRIM(A.NOM_PARENT)) IN ('orange', 'vendeur', 'madame', 'monsieur', 'delta', 'phone', 'inconnu', 'inconnue', 'anonyme', 'unknown')
OR (TRIM(TRANSLATE(LOWER(trim(A.NOM_PARENT)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxMCMLXXXIVcvbnm1234567890¿çéèàäëüïöîôû\'âê-.',' ')) <> ''
AND TRIM(TRANSLATE(LOWER(trim(A.NOM_PARENT)),'*/\?,)([@]_%#}&{    asdfghjklqwertyuiopzxMCMLXXXIVcvbnm1234567890¿çéèàäëüïöîôû\'âê-.',' ')) is not null
)
THEN 'OUI' ELSE 'NON'
END) NOM_PARENT_DOUTEUX,
(CASE WHEN A.DATE_NAISSANCE IS NULL THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_ABSENT,
(CASE WHEN A.DATE_NAISSANCE_TUTEUR IS NULL THEN 'OUI' ELSE 'NON' END) DATE_NAISSANCE_TUT_ABSENT,
(CASE WHEN A.DATE_EXPIRATION IS NULL THEN 'OUI' ELSE 'NON' END) DATE_EXPIRATION_ABSENT,
(CASE WHEN trim(A.ADDRESSE ) = '' OR A.ADDRESSE IS NULL THEN 'OUI' ELSE 'NON' END) ADRESSE_ABSENT,
(CASE WHEN (TRIM(TRANSLATE(LOWER(trim(A.ADDRESSE)), 'abcdefghijklmnopqrstuvwxyz123456MCMLXXXIV()[]_7890çéèàäëüï\'öî¿ôûâê,-./:',' ')) is not null
AND TRIM(TRANSLATE(LOWER(trim(A.ADDRESSE)), 'abcdefghijklmnopqrstuvwxyz123456MCMLXXXIV()[]_7890çéèàäëüïöî¿ôû\'âê,-./:',' ')) <> ''
)
OR (TRIM(TRANSLATE(trim(A.ADDRESSE), '12345678\'90-./:',' ')) =  ''
OR TRIM(TRANSLATE(trim(A.ADDRESSE), '12345678\'90-./:',' ')) is null
)
OR (TRIM(TRANSLATE(LOWER(trim(A.ADDRESSE)), 'bcdfghjklmnpqrstv\'wxzç-./,:',' '))  =  ''
OR TRIM(TRANSLATE(LOWER(trim(A.ADDRESSE)), 'bcdfghjklmnpqrstvw\'xzç-./,:',' ')) is null
)
OR (TRIM(TRANSLATE(LOWER(trim(A.ADDRESSE)), 'aeiouéèàäëüïöîôû\'âê-./:',' '))   =  ''
OR TRIM(TRANSLATE(LOWER(trim(A.ADDRESSE)), 'aeiouéèàäëüïöî\'ôûâê-./:',' ')) is null
)
OR (LENGTH(TRIM(A.ADDRESSE)) < 2 AND not(TRIM(A.ADDRESSE) is null or TRIM(A.ADDRESSE) = ''))
OR TRIM(LOWER(trim(A.ADDRESSE))) IN ('n/a', 'nan', 'unknown', 'inconnue', 'sans adresse','sans','non renseignée') THEN 'OUI' ELSE 'NON'
END) ADRESSE_DOUTEUSE,
(CASE WHEN trim(A.TYPE_PERSONNE) = '' OR A.TYPE_PERSONNE IS NULL THEN 'OUI' ELSE 'NON' END) TYPE_PERSONNE_INCONNU,
null AS MINEUR_MAL_IDENTIFIE,
(CASE WHEN  NOT(trim(G.MSISDN) is null or trim(G.MSISDN) = '') THEN 'OUI' ELSE 'NON' END) EST_PREMIUM,
trim(A.TYPE_PIECE_TUTEUR) AS TYPE_PIECE_TUTEUR,
trim(A.ADRESSE_TUTEUR) AS ADRESSE_TUTEUR,
trim(A.ACCEPTATION_CGV) AS ACCEPTATION_CGV,
trim(A.CONTRAT_SOUCRIPTION) AS CONTRAT_SOUCRIPTION,
trim(A.DISPONIBILITE_SCAN) AS DISPONIBILITE_SCAN,
trim(A.PLAN_LOCALISATION) AS PLAN_LOCALISATION,
trim(A.TYPE_PERSONNE) AS TYPE_PERSONNE_I,
(CASE
WHEN trim(A.DATE_VALIDATION_BO) IS NULL OR trim(A.DATE_VALIDATION_BO) = '' THEN NULL
WHEN trim(A.DATE_VALIDATION_BO) like '%/%'
THEN  from_unixtime(unix_timestamp(translate(SUBSTR(trim(A.DATE_VALIDATION_BO), 1, 19),'/','-'),'yyyy-MM-dd HH:mm:ss'))
WHEN trim(A.DATE_VALIDATION_BO) like '%-%' THEN  from_unixtime(unix_timestamp(SUBSTR(trim(A.DATE_VALIDATION_BO), 1, 19),'yyyy-MM-dd HH:mm:ss'))
ELSE NULL
END) AS DATE_VALIDATION_BO,
trim(A.STATUT_VALIDATION_BO) As STATUT_VALIDATION_BO,
trim(A.MOTIF_REJET_BO) AS MOTIF_REJET_BO,
current_timestamp() AS INSERT_DATE,
to_date('###SLICE_VALUE###') AS EVENT_DATE
FROM (
SELECT msisdn, type_personne, nom_prenom, id_type_piece, type_piece, numero_piece, addresse, quartier, ville, statut_bscs,
statut_validation_bo, motif_rejet_bo, date_validation_bo, login_validateur_bo, canal_validateur_bo, type_abonnement, csmoddate,
ccmoddate, compte_client_structure, nom_structure, numero_registre_commerce, numero_piece_rep_legal, imei, statut_derogation,
region_administrative, region_commerciale, site_name, ville_site, offre_commerciale, type_contrat, segmentation, score_vip,
date_souscription, ville_structure, quartier_structure, raison_statut, prenom, nom, customer_id, contract_id, compte_client,
plan_localisation, contrat_soucription, acceptation_cgv, disponibilite_scan, nom_parent, prenom_tuteur, numero_piece_tuteur,
date_expiration_tuteur, id_type_piece_tuteur, type_piece_tuteur, adresse_tuteur, identificateur, localisation_identificateur,
profession, odbincomingcalls, odboutgoingcalls,
(CASE
WHEN trim(DATE_NAISSANCE) IS NULL OR trim(DATE_NAISSANCE) = '' THEN NULL
WHEN trim(DATE_NAISSANCE) LIKE '%-%'
THEN cast(substr(trim(DATE_NAISSANCE),1,10) as DATE)
WHEN trim(DATE_NAISSANCE) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_NAISSANCE),1,10),'/','-') as DATE)
ELSE NULL
END) DATE_NAISSANCE,
(CASE
WHEN trim(DATE_EXPIRATION) IS NULL OR trim(DATE_EXPIRATION) = '' THEN NULL
WHEN trim(DATE_EXPIRATION) LIKE '%-%'
THEN cast(substr(trim(DATE_EXPIRATION),1,10) AS DATE)
WHEN trim(DATE_EXPIRATION) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_EXPIRATION),1,10),'/','-')  AS DATE)
ELSE NULL
END) DATE_EXPIRATION,
(CASE
WHEN trim(DATE_NAISSANCE_TUTEUR) IS NULL OR trim(DATE_NAISSANCE_TUTEUR) = '' THEN NULL
WHEN trim(DATE_NAISSANCE_TUTEUR) LIKE '%-%'
THEN cast(substr(trim(DATE_NAISSANCE_TUTEUR),1,10)  AS DATE)
WHEN trim(DATE_NAISSANCE_TUTEUR) LIKE '%/%'
THEN cast(translate(substr(trim(DATE_NAISSANCE_TUTEUR),1,10),'/','-')  AS DATE)
ELSE NULL
END) DATE_NAISSANCE_TUTEUR,
(CASE
WHEN trim(DATE_ACTIVATION) IS NULL OR trim(DATE_ACTIVATION) = '' THEN NULL
WHEN trim(DATE_ACTIVATION) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_ACTIVATION), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_ACTIVATION) like '%-%' THEN  cast(SUBSTR(trim(DATE_ACTIVATION), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_ACTIVATION,
(CASE
WHEN trim(DATE_CHANGEMENT_STATUT) IS NULL OR trim(DATE_CHANGEMENT_STATUT) = '' THEN NULL
WHEN trim(DATE_CHANGEMENT_STATUT) like '%/%'
THEN  cast(translate(SUBSTR(trim(DATE_CHANGEMENT_STATUT), 1, 19),'/','-') AS TIMESTAMP)
WHEN trim(DATE_CHANGEMENT_STATUT) like '%-%'
THEN  cast(SUBSTR(trim(DATE_CHANGEMENT_STATUT), 1, 19) AS TIMESTAMP)
ELSE NULL
END) DATE_CHANGEMENT_STATUT
FROM CDR.SPARK_IT_BDI_1A
WHERE ORIGINAL_FILE_DATE =  date_add(to_date('###SLICE_VALUE###'), 1)
) A
LEFT JOIN
(
SELECT AA.*, BB.LONGITUDE, BB.LATITUDE
FROM
(
SELECT * FROM (
SELECT a.*, ROW_NUMBER() OVER (PARTITION  BY msisdn ORDER BY LAST_LOCATION_DAY DESC, insert_date desc) RN
FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY a
WHERE EVENT_DATE = to_date('###SLICE_VALUE###')
) x WHERE RN=1
) AA
LEFT JOIN
(
SELECT *
FROM
(
SELECT DISTINCT
SITE_NAME,
LONGITUDE,
LATITUDE,
ROW_NUMBER() OVER (PARTITION BY SITE_NAME ORDER BY SITE_NAME DESC) AS RANG
FROM DIM.SPARK_DT_GSM_CELL_CODE
) y WHERE RANG = 1
) BB ON upper(trim(AA.SITE_NAME)) = upper(trim(BB.SITE_NAME))
) B ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(B.MSISDN))
LEFT JOIN (SELECT * FROM (SELECT * FROM MON.SPARK_FT_CONTRACT_SNAPSHOT WHERE EVENT_DATE = to_date('###SLICE_VALUE###')) A
LEFT JOIN (
select OFFER_PROFILE_ID, CUSTOMER_TYPE, CUSTOMER_PROFILE, OFFER_NAME, PROFILE_CODE, PROFILE_NAME, INITIAL_CREDIT, RESALE_OF_TRAFFIC, RATEPLAN_ID, PLATFORM, CRM_SEGMENTATION,
DECILE_TYPE, VALID_FROM_DATECODE, VALID_TO_DATECODE, IVR_NUMBER, PROFILE_ID, CONTRACT_TYPE, ESSBASE_SEGMENTATION, ESSBASE_RATEPLAN, OFFER_GROUP, OPERATOR_CODE AS OPERATOR_CODE_DIM,
HORIZON_DOMAIN_CODE, HORIZON_DOMAIN_DESC, HORIZON_MARKET_CODE, HORIZON_MARKET_DESC, HORIZON_OFFER_CODE, HORIZON_OFFER_DESC, SEGMENTATION, OFFRE_B_TO_B, CAT_OFFRE_B_TO_B
from DIM.SPARK_DT_OFFER_PROFILES) B ON upper(trim(A.COMMERCIAL_OFFER)) = upper(trim(B.PROFILE_CODE)) ) C ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(C.ACCESS_KEY))
LEFT JOIN (
SELECT
MDM.MSISDN,
SUM(CASE WHEN MDM.EVENT_MONTH = substr(add_months(to_date('###SLICE_VALUE###'),-3),1,7) THEN NVL (MDM.MAIN_RATED_TEL_AMOUNT,0)+NVL (MDM.MAIN_RATED_SMS_AMOUNT,0)+
NVL (MDM.DATA_MAIN_RATED_AMOUNT,0)+NVL (MDM.TOTAL_SUBS_REVENUE,0)+NVL (MDM.DATA_GOS_MAIN_RATED_AMOUNT,0)+NVL (MDM.SOS_FEES,0)+
NVL (MDM.P2P_REFILL_FEES,0) ELSE 0 END) REV_M_3,
SUM(CASE WHEN MDM.EVENT_MONTH = substr(add_months(to_date('###SLICE_VALUE###'),-2),1,7) THEN NVL (MDM.MAIN_RATED_TEL_AMOUNT,0)+NVL (MDM.MAIN_RATED_SMS_AMOUNT,0)+
NVL (MDM.DATA_MAIN_RATED_AMOUNT,0)+NVL (MDM.TOTAL_SUBS_REVENUE,0)+NVL (MDM.DATA_GOS_MAIN_RATED_AMOUNT,0)+NVL (MDM.SOS_FEES,0)+NVL (MDM.P2P_REFILL_FEES,0)
ELSE 0 END) REV_M_2,
SUM(CASE WHEN MDM.EVENT_MONTH = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7) THEN NVL (MDM.MAIN_RATED_TEL_AMOUNT,0)+NVL (MDM.MAIN_RATED_SMS_AMOUNT,0)+
NVL (MDM.DATA_MAIN_RATED_AMOUNT,0)+NVL (MDM.TOTAL_SUBS_REVENUE,0)+NVL (MDM.DATA_GOS_MAIN_RATED_AMOUNT,0)+NVL (MDM.SOS_FEES,0)+NVL (MDM.P2P_REFILL_FEES,0)
ELSE 0 END) REV_M_1
FROM MON.SPARK_FT_MARKETING_DATAMART_MONTH MDM
WHERE EVENT_MONTH IN (substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)
,substr(add_months(to_date('###SLICE_VALUE###'),-2),1,7)
,substr(add_months(to_date('###SLICE_VALUE###'),-3),1,7))
GROUP BY MSISDN
) D ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(D.MSISDN))
LEFT JOIN (
SELECT
MDM.MSISDN,
SUM(CASE WHEN MDM.MOIS = substr(add_months(to_date('###SLICE_VALUE###'),-3),1,7) THEN NVL (MDM.MAIN_REVENU_TOTAL,0) + NVL (MDM.PROMO_REVENU_TOTAL,0) ELSE 0 END) POST_REV_M_3,
SUM(CASE WHEN MDM.MOIS = substr(add_months(to_date('###SLICE_VALUE###'),-2),1,7) THEN NVL (MDM.MAIN_REVENU_TOTAL,0) + NVL (MDM.PROMO_REVENU_TOTAL,0) ELSE 0 END) POST_REV_M_2,
SUM(CASE WHEN MDM.MOIS = substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7) THEN NVL (MDM.MAIN_REVENU_TOTAL,0) + NVL (MDM.PROMO_REVENU_TOTAL,0) ELSE 0 END) POST_REV_M_1
FROM MON.SPARK_FT_MSISDN_POST_MONTHLY MDM
WHERE MOIS IN (substr(add_months(to_date('###SLICE_VALUE###'),-1),1,7)
,substr(add_months(to_date('###SLICE_VALUE###'),-2),1,7)
,substr(add_months(to_date('###SLICE_VALUE###'),-3),1,7))
GROUP BY MSISDN
) E ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(E.MSISDN))
LEFT JOIN (SELECT DISTINCT MSISDN FROM DIM.SPARK_DT_BDI_DEROGATION) F ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(F.MSISDN))
LEFT JOIN (SELECT DISTINCT MSISDN FROM DIM.SPARK_DT_REF_SEGMENTATION_CLIENT) G ON FN_FORMAT_MSISDN_TO_9DIGITS(trim(A.MSISDN)) = FN_FORMAT_MSISDN_TO_9DIGITS(trim(G.MSISDN))