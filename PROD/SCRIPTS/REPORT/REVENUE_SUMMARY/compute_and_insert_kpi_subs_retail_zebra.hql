INSERT INTO AGG.REVENUE_SUMMARY_DAILY (EVENT_DATE, IN_NBR_VAS_DATA, IN_VAS_DATA_TAX_AMT)
SELECT 
    EVENT_DATE
    , SUM(RATED_COUNT) NBR_VAS_DATA
    , SUM(TAXED_AMOUNT) VAS_DATA_TAX_AMT
FROM 
    (SELECT     
        TRANSACTION_DATE EVENT_DATE
        , COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
        , 'NVX_VAS_DATA' SERVICE_CODE
        , SUM (1) RATED_COUNT
        , SUM (MAIN_AMOUNT) TAXED_AMOUNT
        , OPERATOR_CODE OPERATOR_CODE
    FROM MON.FT_SUBS_RETAIL_ZEBRA
    WHERE  TRANSACTION_DATE = '###SLICE_VALUE###' AND MAIN_AMOUNT > 0
    GROUP BY  
        TRANSACTION_DATE
        , COMMERCIAL_OFFER
        , OPERATOR_CODE
    )A 
LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM' 
GROUP BY
    EVENT_DATE
    ,SERVICE_CODE
;

