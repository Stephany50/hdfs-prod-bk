INSERT INTO TMP.TT_KYC_PERS_PHY_B2C_BDI_LOC
SELECT
MSISDN,
(case
when upper(trim(type_personne))  = 'M2M' then 'M2M'
when upper(trim(OFFRE_COMMERCIALE)) like '%DATALIVE%' then 'M2M'
when upper(trim(OFFRE_COMMERCIALE)) like '%TRACK%' then 'M2M'
when trim(COMPTE_CLIENT) LIKE '4.%' then 'FLOTTE'
else 'PP'
end) as type_personne,
concat_ws(' ',nvl(nom,''),nvl(prenom, '')) as NOM_PRENOM,
ID_TYPE_PIECE,
CASE WHEN trim(ID_TYPE_PIECE)='1' THEN 'CNI'
    WHEN trim(ID_TYPE_PIECE)='2' THEN 'PASSEPORT'
    WHEN trim(ID_TYPE_PIECE)='4' THEN 'CARTE_SEJOUR'
    WHEN trim(ID_TYPE_PIECE)='8' THEN 'AUTRE'
    WHEN trim(ID_TYPE_PIECE)='9' THEN 'ACTE_NAISSANCE'
    WHEN trim(ID_TYPE_PIECE)='12' THEN 'ANCIENNE_CNI'
    WHEN trim(ID_TYPE_PIECE)='13' THEN 'NOUVELLE_CNI'
    WHEN trim(ID_TYPE_PIECE)='10' THEN 'CARTE_MILITAIRE'
    WHEN trim(ID_TYPE_PIECE)='14' THEN 'TITRE_IDENTITE_PROVISOIRE'
    WHEN trim(ID_TYPE_PIECE)='15' THEN 'CARTE_REFUGIE'
    WHEN trim(ID_TYPE_PIECE)='11' THEN 'CARTE_CONSULAIRE' ELSE 'ANCIENNE_CNI'
END TYPE_PIECE   ,
NUMERO_PIECE ,
DATE_EXPIRATION   ,
DATE_NAISSANCE   ,
DATE_ACTIVATION   ,
ADRESSE   ,
QUARTIER   ,
VILLE   ,
STATUT   ,
STATUT_VALIDATION_BO ,
MOTIF_REJET_BO   ,
DATE_VALIDATION_BO   ,
LOGIN_VALIDATEUR_BO   ,
CANAL_VALIDATEUR_BO   ,
TYPE_ABONNEMENT   ,
CSMODDATE     ,
CCMODDATE     ,
COMPTE_CLIENT_STRUCTURE   ,
NOM_STRUCTURE   ,
NUMERO_REGISTRE_COMMERCE   ,
NUMERO_PIECE_REPRESENTANT_LEGAL   ,
IMEI   ,
STATUT_DEROGATION ,
REGION_ADMINISTRATIVE   ,
REGION_COMMERCIALE   ,
SITE_NAME   ,
VILLE_SITE   ,
OFFRE_COMMERCIALE   ,
TYPE_CONTRAT   ,
SEGMENTATION   ,
DATE_SOUSCRIPTION   ,
DATE_CHANGEMENT_STATUT   ,
VILLE_STRUCTURE   ,
QUARTIER_STRUCTURE   ,
RAISON_STATUT   ,
PRENOM   ,
NOM   ,
CUSTOMER_ID   ,
CONTRACT_ID   ,
COMPTE_CLIENT   ,
PLAN_LOCALISATION   ,
CONTRAT_SOUCRIPTION   ,
ACCEPTATION_CGV   ,
DISPONIBILITE_SCAN   ,
NOM_TUTEUR   ,
PRENOM_TUTEUR   ,
DATE_NAISSANCE_TUTEUR   ,
NUMERO_PIECE_TUTEUR   ,
DATE_EXPIRATION_TUTEUR   ,
ID_TYPE_PIECE_TUTEUR   ,
CASE WHEN trim(ID_TYPE_PIECE_TUTEUR)='1' THEN 'CNI'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='2' THEN 'PASSEPORT'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='4' THEN 'CARTE_SEJOUR'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='8' THEN 'AUTRE'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='9' THEN 'ACTE_NAISSANCE'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='12' THEN 'ANCIENNE_CNI'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='13' THEN 'NOUVELLE_CNI'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='10' THEN 'CARTE_MILITAIRE'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='14' THEN 'TITRE_IDENTITE_PROVISOIRE'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='15' THEN 'CARTE_REFUGIE'
    WHEN trim(ID_TYPE_PIECE_TUTEUR)='11' THEN 'CARTE_CONSULAIRE' ELSE 'ANCIENNE_CNI'
END  TYPE_PIECE_TUTEUR   ,
ADRESSE_TUTEUR   ,
IDENTIFICATEUR   ,
LOCALISATION_IDENTIFICATEUR   ,
PROFESSION
FROM  (
SELECT
A.MSISDN  MSISDN,
A.TYPE_PERSONNE     TYPE_PERSONNE ,
A.NOM_PRENOM     NOM_PRENOM ,
A.ID_TYPE_PIECE   ID_TYPE_PIECE,
A.TYPE_PIECE      TYPE_PIECE,
A.NUMERO_PIECE    NUMERO_PIECE,
A.DATE_EXPIRATION      DATE_EXPIRATION,
A.DATE_NAISSANCE      DATE_NAISSANCE,
A.DATE_ACTIVATION      DATE_ACTIVATION,
A.ADRESSE      ADRESSE,
A.QUARTIER      QUARTIER,
A.VILLE      VILLE,
A.STATUT      STATUT,
A.STATUT_VALIDATION_BO  STATUT_VALIDATION_BO,
A.MOTIF_REJET_BO      MOTIF_REJET_BO,
A.DATE_VALIDATION_BO      DATE_VALIDATION_BO,
A.LOGIN_VALIDATEUR_BO     LOGIN_VALIDATEUR_BO ,
A.CANAL_VALIDATEUR_BO      CANAL_VALIDATEUR_BO,
A.TYPE_ABONNEMENT     TYPE_ABONNEMENT ,
A.CSMODDATE   CSMODDATE,
A.CCMODDATE   CCMODDATE,
A.COMPTE_CLIENT_STRUCTURE      COMPTE_CLIENT_STRUCTURE,
A.NOM_STRUCTURE      NOM_STRUCTURE,
A.NUMERO_REGISTRE_COMMERCE      NUMERO_REGISTRE_COMMERCE,
A.NUMERO_PIECE_REPRESENTANT_LEGA      NUMERO_PIECE_REPRESENTANT_LEGAL,
IF(B.msisdn IS NOT NULL , B.IMEI,trim(A.IMEI)) IMEI ,
A.STATUT_DEROGATION    STATUT_DEROGATION,
IF(B.msisdn IS NOT NULL , B.REGION_ADMINISTRATIVE,
trim(A.REGION_ADMINISTRATIVE)) REGION_ADMINISTRATIVE ,
IF(B.msisdn IS NOT NULL , B.REGION_COMMERCIALE,trim(A.REGION_COMMERCIALE)) REGION_COMMERCIALE ,
IF(B.msisdn IS NOT NULL , B.SITE_NAME,trim(A.SITE_NAME)) SITE_NAME ,
IF(B.msisdn IS NOT NULL , B.VILLE_SITE,trim(A.VILLE_SITE))  VILLE_SITE ,
IF(B.msisdn IS NOT NULL , B.OFFRE_COMMERCIALE,trim(A.OFFRE_COMMERCIALE))     OFFRE_COMMERCIALE ,
IF(B.msisdn IS NOT NULL , B.TYPE_CONTRAT,trim(A.TYPE_CONTRAT)) TYPE_CONTRAT  ,
IF(B.msisdn IS NOT NULL , B.SEGMENTATION,trim(A.SEGMENTATION)) SEGMENTATION  ,
A.DATE_SOUSCRIPTION      DATE_SOUSCRIPTION,
A.DATE_CHANGEMENT_STATUT      DATE_CHANGEMENT_STATUT,
A.VILLE_STRUCTURE      VILLE_STRUCTURE,
A.QUARTIER_STRUCTURE      QUARTIER_STRUCTURE,
A.RAISON_STATUT      RAISON_STATUT,
A.PRENOM      PRENOM,
A.NOM      NOM,
A.CUSTOMER_ID      CUSTOMER_ID,
A.CONTRACT_ID      CONTRACT_ID,
A.COMPTE_CLIENT      COMPTE_CLIENT,
A.PLAN_LOCALISATION      PLAN_LOCALISATION,
A.CONTRAT_SOUCRIPTION      CONTRAT_SOUCRIPTION,
A.ACCEPTATION_CGV      ACCEPTATION_CGV,
A.DISPONIBILITE_SCAN      DISPONIBILITE_SCAN,
A.NOM_TUTEUR      NOM_TUTEUR,
A.PRENOM_TUTEUR      PRENOM_TUTEUR,
A.DATE_NAISSANCE_TUTEUR      DATE_NAISSANCE_TUTEUR,
A.NUMERO_PIECE_TUTEUR      NUMERO_PIECE_TUTEUR,
A.DATE_EXPIRATION_TUTEUR     DATE_EXPIRATION_TUTEUR ,
A.ID_TYPE_PIECE_TUTEUR      ID_TYPE_PIECE_TUTEUR,
A.TYPE_PIECE_TUTEUR      TYPE_PIECE_TUTEUR,
A.ADRESSE_TUTEUR      ADRESSE_TUTEUR,
A.IDENTIFICATEUR    IDENTIFICATEUR,
A.LOCALISATION_IDENTIFICATEUR    LOCALISATION_IDENTIFICATEUR  ,
A.PROFESSION    PROFESSION
FROM (select * from TMP.TT_KYC_PERS_PHY_B2C_BDI where not(msisdn is null or trim(msisdn) = '')) A
left join
(SELECT
trim(MSISDN) AS MSISDN,
trim(IMEI) AS IMEI,
trim(SITE_NAME) AS SITE_NAME,
trim(COMMERCIAL_REGION) AS REGION_COMMERCIALE,
trim(ADMINISTRATIVE_REGION) AS REGION_ADMINISTRATIVE,
trim(TOWNNAME) AS VILLE_SITE,
trim(COMMERCIAL_OFFER) AS OFFRE_COMMERCIALE,
trim(CONTRACT_TYPE) AS TYPE_CONTRAT,
trim(SEGMENTATION) AS SEGMENTATION
FROM MON.SPARK_FT_MSISDN_IMEI_LOCALISATION_TO_BDI
WHERE EVENT_DATE in (select max(event_date) from MON.SPARK_FT_MSISDN_IMEI_LOCALISATION_TO_BDI)
 and not(msisdn is null or trim(msisdn) = '')) B
ON FN_FORMAT_MSISDN_TO_9DIGITS(TRIM(A.MSISDN))=FN_FORMAT_MSISDN_TO_9DIGITS(TRIM(B.MSISDN))
) T