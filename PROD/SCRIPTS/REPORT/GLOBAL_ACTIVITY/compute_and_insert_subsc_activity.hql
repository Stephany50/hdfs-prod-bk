INSERT INTO AGG.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
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
FROM AGG.spark_FT_A_SUBSCRIPTION
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND SUBS_AMOUNT > 0
 AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE 'PREPAID INDIVIDUAL FORFAIT%'
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
;
