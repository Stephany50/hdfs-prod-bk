SELECT
    'CIP.MM.01' NATURE_DECLARATION,
    '62402' CODE_OPERATEUR,
    (CASE 
         WHEN B.TOWNNAME = 'YAOUNDE' THEN 10
         WHEN B.TOWNNAME = 'DOUALA' THEN 11
         WHEN B.TOWNNAME = 'GAROUA' THEN 12
         WHEN B.TOWNNAME = 'LIMBE' THEN 13
         WHEN B.TOWNNAME = 'NKONGSAMBA' THEN 14
         WHEN B.TOWNNAME = 'BAFOUSSAM' THEN 15
         WHEN B.TOWNNAME = 'NGAOUNDERE' THEN 17
         WHEN B.TOWNNAME = 'BAMENDA' THEN 18
         ELSE 19
     END) CODE_VILLE,
    'OCM' SIGLE_OPERATEUR,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN '' ELSE A.USER_ID END) CODE_MARCHAND,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN '' ELSE UPPER(A.USER_FIRST_NAME) END) NOM_MARCHAND,
    A.USER_ID NUMERO_COMPTE,
    from_unixtime(unix_timestamp(A.CREATION_DATE), 'dd/MM/yyyy') DATE_OUVERTURE_CPTE,
    UPPER(A.USER_TYPE) TYPE_COMPTE,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN A.ID_NUMBER ELSE '' END) CNI_TITULAIRE,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN 'PHYSIQUE' ELSE 'MORALE' END) TYPE_PERSONNE,
    '' FORME_JURIDIQUE,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN '' ELSE UPPER(A.USER_FIRST_NAME) END) NOM_PERSONNE_MORALE,
    '' DATE_CREATION_PERS_MORALE,
    B.TOWNNAME ADRESSE,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN UPPER(A.USER_LAST_NAME) ELSE '' END) NOM_PERSONNE_PHYSIQUE,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN UPPER(A.USER_FIRST_NAME) ELSE '' END) PRENOM_PERSONNE_PHYSIQUE,
    '' BOITE_POSTALE,
    '' E_MAIL,
    from_unixtime(unix_timestamp(A.DOB, 'yyyy-MM-dd'), 'dd/MM/yyyy') DATE_NAISSANCE,
    '' LIEU_NAISSANCE,
    '' RCCM_PERSONNE_MORALE,
    'XAF' MONNAIE_COMPTE,
    (CASE WHEN A.USER_TYPE = 'SUBSCRIBER' THEN '' ELSE A.PARENT_USER_ID END) NUMERO_COMPTE_MARCHAND_PARENT,
    '0' INTERNATIONAL
FROM (
    SELECT *
    FROM cdr.spark_it_om_subscribers
    WHERE REGISTERED_ON like '###SLICE_VALUE###%' and modification_date >= '###SLICE_VALUE###'
    AND ACTION_TYPE IN ('CREATION', 'ACTIVATION')
) A
LEFT JOIN (
SELECT * FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
WHERE EVENT_DATE=(SELECT MAX(EVENT_DATE) FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY WHERE EVENT_DATE between date_sub('###SLICE_VALUE###', 7) AND '###SLICE_VALUE###')
) B ON A.MSISDN = B.MSISDN
ORDER BY A.USER_TYPE, A.PARENT_USER_ID
