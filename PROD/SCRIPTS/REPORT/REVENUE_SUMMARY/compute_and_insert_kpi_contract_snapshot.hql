INSERT INTO AGG.REVENUE_SUMMARY_DAILY  (EVENT_DATE, IN_NBR_DEAC_ACCT_BAL, IN_DEAC_ACCT_BAL_TAX_AMT)
SELECT 
    EVENT_DATE
    , SUM(RATED_COUNT) NBR_DEAC_ACCT_BAL
    , SUM(TAXED_AMOUNT) DEAC_ACCT_BAL_TAX_AMT
FROM 
    (SELECT     
        DEACTIVATION_DATE EVENT_DATE
        , UPPER(PROFILE) COMMERCIAL_OFFER_CODE
        , 'NVX_BALANCE' SERVICE_CODE
        , SUM (1) RATED_COUNT
        , SUM (MAIN_CREDIT) TAXED_AMOUNT
        , OPERATOR_CODE OPERATOR_CODE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'  AND MAIN_CREDIT > 0
    GROUP BY  
        DEACTIVATION_DATE
        ,UPPER(PROFILE)
        , OPERATOR_CODE
    )A 
LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM' 
GROUP BY
    EVENT_DATE
    ,SERVICE_CODE;

