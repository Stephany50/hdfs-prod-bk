INSERT INTO REPORT.REVENUE_SUMMARY_DAILY (EVENT_DATE, IN_NBR_DATA_TRANS, IN_DATA_TRANS_TAX_AMT) 
SELECT 
    EVENT_DATE
    , SUM(RATED_COUNT) NBR_DATA_TRANS
    , SUM(TAXED_AMOUNT) DATA_TRANS_TAX_AMT
FROM 
    (SELECT     
        EVENT_DATE EVENT_DATE
        , SENDER_OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
        , 'NVX_P2P_DATA' SERVICE_CODE
        , SUM (1) RATED_COUNT
        , SUM (MONTANT_FRAIS) TAXED_AMOUNT
        , SENDER_OPERATOR_CODE OPERATOR_CODE
    FROM MON.FT_A_DATA_TRANSFER
    WHERE  EVENT_DATE = '###SLICE_VALUE###' AND MONTANT_FRAIS > 0                       
    GROUP BY  
        EVENT_DATE
        , SENDER_OFFER_PROFILE_CODE
        , SENDER_OPERATOR_CODE
    )A 
LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM' 
GROUP BY
    EVENT_DATE
    ,SERVICE_CODE
;

