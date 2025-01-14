INSERT INTO agg.REVENUE_SUMMARY_DAILY  (EVENT_DATE, TANGO_NBR_OM_DATA, TANGO_OM_DATA_TAX_AMT)
SELECT
    EVENT_DATE
    , SUM(RATED_COUNT) NBR_OM_DATA
    , SUM(TAXED_AMOUNT) OM_DATA_TAX_AMT
FROM 
    (SELECT     
        TO_DATE(TRANSFER_DATETIME) EVENT_DATE
        , B.PROFILE COMMERCIAL_OFFER_CODE
        , 'NVX_OM_DATA' SERVICE_CODE
        , SUM (1) RATED_COUNT
        , SUM (TRANSACTION_AMOUNT) TAXED_AMOUNT
        , B.OPERATOR_CODE OPERATOR_CODE
    FROM CDR.IT_OM_TRANSACTIONS A
    LEFT JOIN (SELECT ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
               FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
               LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                          WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                          GROUP BY ACCESS_KEY) B 
                ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
               WHERE B.ACCESS_KEY IS NOT NULL                 
               GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE) B
    ON A.SENDER_MSISDN = B.ACCESS_KEY
    WHERE  TRANSACTION_DATE = '###SLICE_VALUE###' 
           AND  TO_DATE(TRANSFER_DATETIME) = '###SLICE_VALUE###' AND TRANSFER_DONE = 'Y' AND RECEIVER_MSISDN IN ('698066666', '658101010', '658121212')                      
    GROUP BY  
        TO_DATE(TRANSFER_DATETIME)
        , B.PROFILE
        , B.OPERATOR_CODE
    )A 
LEFT JOIN (SELECT PROFILE_CODE, UPPER(SEGMENTATION) SEGMENTATION FROM DIM.DT_OFFER_PROFILES) B ON B.PROFILE_CODE=UPPER(A.COMMERCIAL_OFFER_CODE)
WHERE B.SEGMENTATION  IN  ('STAFF','B2B','B2C') AND A.OPERATOR_CODE = 'OCM' 
GROUP BY
    EVENT_DATE
    ,SERVICE_CODE
;

