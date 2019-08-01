INSERT INTO AGG.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT     
     B.PROFILE COMMERCIAL_OFFER_CODE
    ,'OM_DATA' TRANSACTION_TYPE
    ,'MAIN' SUB_ACCOUNT
    ,'-' TRANSACTION_SIGN
    ,'TANGO' SOURCE_PLATFORM
    ,'IT_OM_TRANSACTIONS'  SOURCE_DATA
    ,'OM_DATA' SERVED_SERVICE
    , 'NVX_OM_DATA' SERVICE_CODE
     ,'DEST_ND' DESTINATION_CODE
    ,NULL SERVED_LOCATION
    ,'HIT' MEASUREMENT_UNIT
    , SUM (1) RATED_COUNT
    , SUM (1) RATED_VOLUME
    , SUM (TRANSACTION_AMOUNT) TAXED_AMOUNT
    , SUM ((1-0.1925) *TRANSACTION_AMOUNT) UNTAXED_AMOUNT
    , CURRENT_TIMESTAMP INSERT_DATE
    ,'REVENUE' TRAFFIC_MEAN
    , B.OPERATOR_CODE OPERATOR_CODE
    , TO_DATE(TRANSFER_DATETIME) TRANSACTION_DATE
FROM CDR.IT_OM_TRANSACTIONS A
LEFT JOIN (SELECT ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE 
            FROM MON.FT_CONTRACT_SNAPSHOT A
            LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.FT_CONTRACT_SNAPSHOT
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
;
