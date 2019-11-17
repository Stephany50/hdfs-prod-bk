-- Insert data into aggregat table
INSERT INTO/*append*/ MON.FT_A_BDI_V2
SELECT
    A.EVENT_DATE,
    NVL(A.TYPE_PERSONNE, 'NA') TYPE_PERSONNE,
    A.REGION_ADMINISTRATIVE REGION,
    A.VILLE,
    A.SITE_NAME SITE,
    MAX(A.LONGITUDE) LONGITUDE,
    MAX(A.LATITUDE) LATITUDE,
    SUM(CASE WHEN A.EST_PRESENT_OCM = 'OUI' THEN 1 ELSE 0 END) NB_PARC_COMMERCIAL,
    SUM(CASE WHEN A.EST_PRESENT_GP = 'OUI' THEN 1 ELSE 0 END) NB_PARC_GROUPE,
    SUM(CASE WHEN A.EST_PRESENT_ART = 'OUI' THEN 1 ELSE 0 END) NB_PARC_ART,
    COUNT(DISTINCT A.MSISDN) NB_BDI,
    SUM(CASE WHEN A.STATUT_BSCS = 'ACTIF' THEN 1 ELSE 0 END) NB_ACTIF_HLR,
    SUM(CASE WHEN A.EST_PREMIUM = 'OUI' THEN 1 ELSE 0 END) NB_PREMIUM,
    SUM(CASE WHEN A.EST_CLIENT_VIP = 'OUI' THEN 1 ELSE 0 END) NB_VIP,
    SUM(CASE WHEN A.STATUT_DEROGATION = 'OUI' THEN 1 ELSE 0 END) NB_AVEC_DEROGATION,
    SUM(CASE WHEN TRUNC(A.DATE_ACTIVATION) = A.EVENT_DATE THEN 1 ELSE 0 END) NB_ACTIVATION,
    SUM(CASE WHEN TRUNC(A.DATE_ACTIVATION) = A.EVENT_DATE AND A.CONFORME_ART = 'NON' THEN 1 ELSE 0 END) NB_ACTIVATION_MAL_IDENTIF,
    SUM(CASE WHEN A.CONFORME_ART = 'NON' THEN 1 ELSE 0 END) NB_MAL_IDENTIF,
    SUM(CASE WHEN A.MULTI_SIM = 'OUI' THEN 1 ELSE 0 END) NB_MAL_IDENTIF_MULTI_SIM,
    SUM(CASE WHEN A.CNI_EXPIRE = 'OUI' THEN 1 ELSE 0 END) NB_MAL_IDENTIF_CNI_EXPIRE,
    SUM(CASE WHEN A.CONFORME_ART = 'NON' AND A.EST_PRESENT_ZEB = 'OUI' THEN 1 ELSE 0 END) NB_MAL_IDENTIF_ZEB,
    SUM(CASE WHEN A.CONFORME_ART = 'NON' AND A.EST_PREMIUM = 'OUI' THEN 1 ELSE 0 END) NB_MAL_IDENTIF_PREMIUM,
    SUM(CASE WHEN A.CONFORME_ART = 'NON' AND A.EST_CLIENT_VIP = 'OUI' THEN 1 ELSE 0 END) NB_MAL_IDENTIF_VIP,
    SUM(CASE WHEN A.CONFORME_ART = 'NON' AND A.STATUT_DEROGATION = 'OUI' THEN 1 ELSE 0 END) NB_MAL_IDENTIF_DEROGATION,
    SUM(CASE WHEN A.CONFORME_ART = 'OUI' AND B.CONFORME_ART = 'NON'  THEN 1 ELSE 0 END) NB_CORRECTION,
    0 NB_SANS_SCAN
FROM
(
    SELECT *
    FROM MON.FT_BDI
    WHERE EVENT_DATE ='2019-10-16'
    AND EST_SUSPENDU ='NON'
) A
LEFT JOIN
(
    SELECT *
    FROM MON.FT_BDI
    WHERE EVENT_DATE = (SELECT MAX(EVENT_DATE) FROM MON.FT_A_BDI WHERE EVENT_DATE < '2019-10-16')
    AND EST_SUSPENDU ='NON'
) B ON A.MSISDN = B.MSISDN
GROUP BY A.EVENT_DATE, A.REGION_ADMINISTRATIVE, A.VILLE, A.SITE_NAME, NVL(A.TYPE_PERSONNE, 'NA');
