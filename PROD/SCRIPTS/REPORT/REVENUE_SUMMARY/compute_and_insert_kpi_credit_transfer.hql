INSERT INTO REPORT.REVENUE_SUMMARY_DAILY (EVENT_DATE, P2P_NBR_TRANS_FEES, P2P_TRANS_FEES_TAX_AMT, P2P_NBR_CREDIT_TRANS, P2P_CREDIT_TRANS_TAX_AMT) 
SELECT 
    EVENT_DATE
    , SUM(RATED_COUNT) NBR_TRANS_FEES
    , SUM(FEES_TAXED_AMOUNT) TRANS_FEES_TAX_AMT
    , SUM(RATED_COUNT) NBR_CREDIT_TRANS
    , SUM(TAXED_AMOUNT) CREDIT_TRANS_TAX_AMT
FROM 
    (SELECT     
        REFILL_DATE EVENT_DATE
        , COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
        , 'NVX_P2P' SERVICE_CODE
        , SUM (1) RATED_COUNT
        , SUM (TRANSFER_AMT) TAXED_AMOUNT
        , SUM (TRANSFER_FEES) FEES_TAXED_AMOUNT
        , SENDER_OPERATOR_CODE OPERATOR_CODE
    FROM MON.FT_CREDIT_TRANSFER
    WHERE REFILL_DATE = '###SLICE_VALUE###' AND TERMINATION_IND = '000'
    GROUP BY  
        REFILL_DATE
        , COMMERCIAL_OFFER
        , SENDER_OPERATOR_CODE
    )A 
LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM' 
GROUP BY
    EVENT_DATE
    ,SERVICE_CODE
;

