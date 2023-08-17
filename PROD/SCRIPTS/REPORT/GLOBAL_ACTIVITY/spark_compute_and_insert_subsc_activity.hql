INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
    UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
     ,(CASE
           WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
               OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
               OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
               OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
               OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
               OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
               OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'USSD_SUBSCRIPTION'
           ELSE 'USSD'
    END) TRANSACTION_TYPE
     ,'MAIN' SUB_ACCOUNT
     ,'+' TRANSACTION_SIGN
     , 'ZTE' SOURCE_PLATFORM
     ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
     , 'IN_TRAFFIC' SERVED_SERVICE
     , (CASE
        WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%DEAL%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
        WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
        WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
        ELSE 'BUN_VOX' /* New individual price plan*/
       END) SERVICE_CODE
     , 'DEST_ND' DESTINATION_CODE
     , NULL SERVED_LOCATION
     ,'HIT' MEASUREMENT_UNIT
     , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
     , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
     , SUM (SUBS_AMOUNT) TAXED_AMOUNT
     , SUM ((1-0.1925) * SUBS_AMOUNT) UNTAXED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,'REVENUE' TRAFFIC_MEAN
     , OPERATOR_CODE OPERATOR_CODE
     , NULL LOCATION_CI
     , TRANSACTION_DATE TRANSACTION_DATE
    FROM AGG.SPARK_FT_A_SUBSCRIPTION
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND SUBS_AMOUNT > 0
  AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE 'PREPAID INDIVIDUAL FORFAIT%' and  subs_channel <>'32' and  (upper(subs_channel) not like '%GOS%SDP%' or upper(SUBS_BENEFIT_NAME) not like '%MY%WAY%DIGITAL%')
GROUP BY
    TRANSACTION_DATE
       , UPPER(COMMERCIAL_OFFER)
       ,(CASE
             WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
                 OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                 OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                 OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                 OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                 OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                 OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'USSD_SUBSCRIPTION'
             ELSE 'USSD'
    END)
       , (CASE
        WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%DEAL%'
                OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
        WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
        WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
        ELSE 'BUN_VOX' /* New individual price plan*/
       END)
       , OPERATOR_CODE
UNION ALL
SELECT
   UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
   ,'OM_DATA' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'+' TRANSACTION_SIGN
   , 'ZTE' SOURCE_PLATFORM
   ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
   , 'IN_TRAFFIC' SERVED_SERVICE
   , 'NVX_OM_DATA' SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   ,'HIT' MEASUREMENT_UNIT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , OPERATOR_CODE OPERATOR_CODE
   , NULL LOCATION_CI
   , DATECODE TRANSACTION_DATE
FROM
(
    SELECT DATECODE
        , BENEFIT_NAME
        , PRICE_PLAN_NAME
        , ((COEFF_DATA + COEFF_ROAMING_DATA)/100)*SUBS_EVENT_RATED_COUNT SUBS_EVENT_RATED_COUNT
        , ((COEFF_DATA + COEFF_ROAMING_DATA)/100)*TOTAL_AMOUNT TOTAL_AMOUNT
        , SUBS_CHANNEL
        , SUBS_SERVICE
        , OPERATOR_CODE
    FROM 
    (
        SELECT  TRANSACTION_DATE AS DATECODE,
            SUBS_BENEFIT_NAME AS BENEFIT_NAME,
            COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
            SUBS_EVENT_RATED_COUNT,
            SUBS_AMOUNT AS TOTAL_AMOUNT,
            SUBS_CHANNEL,
            SUBS_SERVICE,
            OPERATOR_CODE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION
        WHERE TRANSACTION_DATE ='###SLICE_VALUE###' and (subs_channel in ('32', '111') or (upper(subs_channel) like '%GOS%SDP%' and upper(SUBS_BENEFIT_NAME) like '%MY%WAY%DIGITAL%') )
    ) X0
    LEFT JOIN 
    DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1
    ON UPPER(TRIM(X0.BENEFIT_NAME)) = UPPER(TRIM(X1.BDLE_NAME))
)T
WHERE
    TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
     AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
GROUP BY
     DATECODE
    ,UPPER(PRICE_PLAN_NAME)
    ,'OM_DATA'
    ,'MAIN'
    ,'+'
    , 'ZTE'
    ,'FT_A_SUBSCRIPTION'
    , 'IN_TRAFFIC'
    , 'NVX_OM_DATA'
    , 'DEST_ND'
    , NULL
    ,'HIT'
    ,'REVENUE'
    , OPERATOR_CODE

UNION ALL
SELECT
   UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
   ,'OM_VOICE' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'+' TRANSACTION_SIGN
   , 'ZTE' SOURCE_PLATFORM
   ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
   , 'IN_TRAFFIC' SERVED_SERVICE
   , 'NVX_OM_VOICE' SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   ,'HIT' MEASUREMENT_UNIT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , OPERATOR_CODE OPERATOR_CODE
   , NULL LOCATION_CI
   , DATECODE TRANSACTION_DATE
FROM
(
    SELECT DATECODE
        , BENEFIT_NAME
        , PRICE_PLAN_NAME
        , ((COEFF_ONNET + COEFF_OFFNET + COEFF_INTER + COEFF_ROAMING_VOIX)/100)*SUBS_EVENT_RATED_COUNT SUBS_EVENT_RATED_COUNT
        , ((COEFF_ONNET + COEFF_OFFNET + COEFF_INTER + COEFF_ROAMING_VOIX)/100)*TOTAL_AMOUNT TOTAL_AMOUNT
        , SUBS_CHANNEL
        , SUBS_SERVICE
        , OPERATOR_CODE
    FROM 
    (
        SELECT  TRANSACTION_DATE AS DATECODE,
            SUBS_BENEFIT_NAME AS BENEFIT_NAME,
            COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
            SUBS_EVENT_RATED_COUNT,
            SUBS_AMOUNT AS TOTAL_AMOUNT,
            SUBS_CHANNEL,
            SUBS_SERVICE,
            OPERATOR_CODE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION
        WHERE TRANSACTION_DATE ='###SLICE_VALUE###' and (subs_channel in ('32', '111') or (upper(subs_channel) like '%GOS%SDP%' and upper(SUBS_BENEFIT_NAME) like '%MY%WAY%DIGITAL%') )
    ) X0
    LEFT JOIN 
    DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1
    ON UPPER(TRIM(X0.BENEFIT_NAME)) = UPPER(TRIM(X1.BDLE_NAME))
)T
WHERE
    TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
     AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
GROUP BY
     DATECODE
    ,UPPER(PRICE_PLAN_NAME)
    ,'OM_VOICE'
    ,'MAIN'
    ,'+'
    , 'ZTE'
    ,'FT_A_SUBSCRIPTION'
    , 'IN_TRAFFIC'
    , 'NVX_OM_VOICE'
    , 'DEST_ND'
    , NULL
    ,'HIT'
    ,'REVENUE'
    , OPERATOR_CODE


UNION ALL
SELECT
   UPPER(PRICE_PLAN_NAME) COMMERCIAL_OFFER_CODE
   ,'OM_SMS' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'+' TRANSACTION_SIGN
   , 'ZTE' SOURCE_PLATFORM
   ,'FT_A_SUBSCRIPTION'  SOURCE_DATA
   , 'IN_TRAFFIC' SERVED_SERVICE
   , 'NVX_OM_SMS' SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   ,'HIT' MEASUREMENT_UNIT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
   , SUM (SUBS_EVENT_RATED_COUNT) RATED_VOLUME
   , SUM (TOTAL_AMOUNT) TAXED_AMOUNT
   , SUM (TOTAL_AMOUNT-TOTAL_AMOUNT*0.1925) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , OPERATOR_CODE OPERATOR_CODE
   , NULL LOCATION_CI
   , DATECODE TRANSACTION_DATE
FROM
(
    SELECT DATECODE
        , BENEFIT_NAME
        , PRICE_PLAN_NAME
        , ((COEF_SMS + COEFF_ROAMING_SMS)/100)*SUBS_EVENT_RATED_COUNT SUBS_EVENT_RATED_COUNT
        , ((COEF_SMS + COEFF_ROAMING_SMS)/100)*TOTAL_AMOUNT TOTAL_AMOUNT
        , SUBS_CHANNEL
        , SUBS_SERVICE
        , OPERATOR_CODE
    FROM 
    (
        SELECT  TRANSACTION_DATE AS DATECODE,
            SUBS_BENEFIT_NAME AS BENEFIT_NAME,
            COMMERCIAL_OFFER AS PRICE_PLAN_NAME,
            SUBS_EVENT_RATED_COUNT,
            SUBS_AMOUNT AS TOTAL_AMOUNT,
            SUBS_CHANNEL,
            SUBS_SERVICE,
            OPERATOR_CODE
        FROM AGG.SPARK_FT_A_SUBSCRIPTION
        WHERE TRANSACTION_DATE ='###SLICE_VALUE###' and (subs_channel in ('32', '111') or (upper(subs_channel) like '%GOS%SDP%' and upper(SUBS_BENEFIT_NAME) like '%MY%WAY%DIGITAL%') )
    ) X0
    LEFT JOIN 
    DIM.DT_CBM_REF_SOUSCRIPTION_PRICE X1
    ON UPPER(TRIM(X0.BENEFIT_NAME)) = UPPER(TRIM(X1.BDLE_NAME))
)T
WHERE
    TOTAL_AMOUNT>0 --ne prendre que les évènements facturés
     AND NVL(UPPER(BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'--eliminer les souscriptions au forfait500,1000 et 5000
GROUP BY
     DATECODE
    ,UPPER(PRICE_PLAN_NAME)
    ,'OM_SMS'
    ,'MAIN'
    ,'+'
    , 'ZTE'
    ,'FT_A_SUBSCRIPTION'
    , 'IN_TRAFFIC'
    , 'NVX_OM_SMS'
    , 'DEST_ND'
    , NULL
    ,'HIT'
    ,'REVENUE'
    , OPERATOR_CODE