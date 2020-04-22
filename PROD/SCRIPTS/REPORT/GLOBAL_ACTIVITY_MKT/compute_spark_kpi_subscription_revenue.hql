--Insertion du Revenu des souscriptions voix (Orange Bundle)
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
'REVENUE_VOICE_BUNDLE' DESTINATION_CODE
,COMMERCIAL_OFFER PROFILE_CODE
,'MAIN' SUB_ACCOUNT
,'HIT' MEASUREMENT_UNIT
, 'FT_A_SUBSCRIPTION' SOURCE_TABLE
,OPERATOR_CODE
,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
,SUM(SUBS_AMOUNT) RATED_AMOUNT
,CURRENT_TIMESTAMP INSERT_DATE
,NULL REGION_ID
,TRANSACTION_DATE
FROM AGG.SPARK_FT_A_SUBSCRIPTION
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
AND (
        UPPER(SUBS_BENEFIT_NAME)='PREPAID INDIVIDUAL BUNDLE MONEY'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'FORFAITS ORANGE%'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'FORFAIT ORANGE%'
     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP SASSAYE JEUNESSE'
     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP FAMILYTALK FOR SUNDAY TALK'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP MAXI BONUS%'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS ALLNET%'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS INTER%'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS ONNET%'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP ORANGE BONUS SUPER%'
     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP ORANGE BONUS CLASSIQUE'
     OR UPPER(SUBS_BENEFIT_NAME) LIKE 'IPP GALAXY%'
     OR UPPER(SUBS_BENEFIT_NAME) = 'INTERNETCM MOBILE ANNEE'
     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP FLYBOX CONFORT'
     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP INTERNATIONAL HAJJ JOUR'
     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP ORANGE BONUS NEW RATING_0'
     OR UPPER(SUBS_BENEFIT_NAME) = 'IPP PREPAID MULTI SIM TRAP'
     )
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE

UNION ALL
      --Insertion du Nombre des souscriptions voix (Usage Orange Bundle)
SELECT
    'USAGE_VOICE_BUNDLE' DESTINATION_CODE
    ,FORMULE PROFILE_CODE
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(BUNDLE_TEL_DURATION) / 60 TOTAL_AMOUNT
    ,SUM(BUNDLE_TEL_DURATION) / 60 RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,NULL REGION_ID
    ,EVENT_DATE TRANSACTION_DATE
FROM MON.SPARK_FT_CONSO_MSISDN_DAY
WHERE EVENT_DATE  = '###SLICE_VALUE###'
GROUP BY EVENT_DATE
        ,FORMULE
        ,OPERATOR_CODE

UNION ALL
--Insertion du Revenu des souscriptions SMS (Orange Bundle)
SELECT
    'REVENUE_SMS_BUNDLE' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_A_SUBSCRIPTION' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,NULL REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_A_SUBSCRIPTION
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND UPPER(SUBS_BENEFIT_NAME) LIKE '%SMS%'
    GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE

UNION ALL
--Insertion du Nombre de souscriptions SMS (Orange Bundle SMS)
SELECT
    'USAGE_SMS_BUNDLE' DESTINATION_CODE
    ,FORMULE PROFILE_CODE
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(BUNDLE_SMS_COUNT) TOTAL_AMOUNT
    ,SUM(BUNDLE_SMS_COUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,NULL REGION_ID
    ,EVENT_DATE TRANSACTION_DATE
FROM MON.SPARK_FT_CONSO_MSISDN_DAY
WHERE EVENT_DATE = '###SLICE_VALUE###'
GROUP BY EVENT_DATE
    ,FORMULE
    ,OPERATOR_CODE


UNION ALL

--Insertion du Revenu des souscriptions Data (2G Bundle)
SELECT
    'REVENUE_2G_BUNDLE' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,NULL REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_A_SUBSCRIPTION
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND  (NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%')
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE


UNION ALL

--Insertion du Revenu des souscriptions Data (3G Bundle)

SELECT
    'REVENUE_3G_BUNDLE' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,NULL REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_A_SUBSCRIPTION
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND  (NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%')
    GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE