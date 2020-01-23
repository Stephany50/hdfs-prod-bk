INSERT INTO AGG.SPARK_REVENUE_SUMMARY_DAILY  (EVENT_DATE, IN_NBR_SUBSC_BUN_SMS, IN_SUBSC_BUN_SMS_TAX_AMT, IN_NBR_SUBSC_MOD_FNF, IN_SUBSC_MOD_FNF_TAX_AMT
                                       ,IN_NBR_SUBSC_CHG_BRAND, IN_SUBSC_CHG_BRAND_TAX_AMT, IN_NBR_SUBSC_USS, IN_SUBSC_USS_TAX_AMT, IN_NBR_SUBSC_BUN_VOX
                                       ,IN_SUBSC_BUN_VOX_TAX_AMT )
SELECT
    EVENT_DATE
     , SUM(NBR_SUBSC_BUN_SMS) NBR_SUBSC_BUN_SMS
     , SUM(SUBSC_BUN_SMS_TAX_AMT) SUBSC_BUN_SMS_TAX_AMT
     , SUM(NBR_SUBSC_MOD_FNF) NBR_SUBSC_MOD_FNF
     , SUM(SUBSC_MOD_FNF_TAX_AMT) SUBSC_MOD_FNF_TAX_AMT
     , SUM(NBR_SUBSC_CHG_BRAND) NBR_SUBSC_CHG_BRAND
     , SUM(SUBSC_CHG_BRAND_TAX_AMT) SUBSC_CHG_BRAND_TAX_AMT
     , SUM(NBR_SUBSC_USS) NBR_SUBSC_USS
     , SUM(SUBSC_USS_TAX_AMT) SUBSC_USS_TAX_AMT
     , SUM(NBR_SUBSC_BUN_VOX) NBR_SUBSC_BUN_VOX
     , SUM(SUBSC_BUN_VOX_TAX_AMT) SUBSC_BUN_VOX_TAX_AMT
FROM(
        SELECT
            EVENT_DATE
             , SUM(IF(SERVICE_CODE = 'BUN_SMS', RATED_COUNT, 0)) NBR_SUBSC_BUN_SMS
             , SUM(IF(SERVICE_CODE = 'BUN_SMS',TAXED_AMOUNT, 0)) SUBSC_BUN_SMS_TAX_AMT
             , SUM(IF(SERVICE_CODE = '122', RATED_COUNT, 0)) NBR_SUBSC_MOD_FNF
             , SUM(IF(SERVICE_CODE = '122',TAXED_AMOUNT, 0)) SUBSC_MOD_FNF_TAX_AMT
             , SUM(IF(SERVICE_CODE = '35', RATED_COUNT, 0)) NBR_SUBSC_CHG_BRAND
             , SUM(IF(SERVICE_CODE = '35',TAXED_AMOUNT, 0)) SUBSC_CHG_BRAND_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'NVX_USS', RATED_COUNT, 0)) NBR_SUBSC_USS
             , SUM(IF(SERVICE_CODE = 'NVX_USS',TAXED_AMOUNT, 0)) SUBSC_USS_TAX_AMT
             , SUM(IF(SERVICE_CODE = 'BUN_VOX', RATED_COUNT, 0)) NBR_SUBSC_BUN_VOX
             , SUM(IF(SERVICE_CODE = 'BUN_VOX',TAXED_AMOUNT, 0)) SUBSC_BUN_VOX_TAX_AMT
        FROM
            (SELECT
                 TRANSACTION_DATE EVENT_DATE
                  ,COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
                  ,(CASE
                        WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                            OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
                        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
                        WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
                        WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
                        WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
                        ELSE 'BUN_VOX' /* New individual price plan*/
                    END) SERVICE_CODE
                  , SUM (SUBS_EVENT_RATED_COUNT) RATED_COUNT
                  , SUM (SUBS_AMOUNT) TAXED_AMOUNT
                  , OPERATOR_CODE OPERATOR_CODE
             FROM AGG.SPARK_FT_A_SUBSCRIPTION
             WHERE TRANSACTION_DATE = '###SLICE_VALUE###'  AND NVL(SUBS_AMOUNT,0) > 0 AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') not like 'PREPAID INDIVIDUAL FORFAIT%'
             GROUP BY
                 TRANSACTION_DATE
                    ,COMMERCIAL_OFFER
                    ,(CASE
                          WHEN NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'DATACM%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'BLACKBERRY%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'ORANGECM%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP BROADBAND 3G%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%DATA%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP ORANGE BONUS DATA%'
                              OR  NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE 'IPP%3G%' THEN 'NVX_USS'
                          WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') LIKE '%SMS%' THEN 'BUN_SMS'
                          WHEN SUBS_SERVICE = 'New Individual Price Plan' AND NVL(UPPER(SUBS_BENEFIT_NAME),'ND') NOT LIKE '%SMS%' THEN 'BUN_VOX'
                          WHEN SUBS_SERVICE = 'Change Main Product(Brand)' THEN '35'
                          WHEN SUBS_SERVICE = 'Modify FnF Number' THEN '122'
                          ELSE 'BUN_VOX' /* New individual price plan*/
                 END)
                    ,OPERATOR_CODE
            )A
                LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
        WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM'
        GROUP BY
            EVENT_DATE
               ,SERVICE_CODE
    ) T
GROUP BY
    EVENT_DATE
;
