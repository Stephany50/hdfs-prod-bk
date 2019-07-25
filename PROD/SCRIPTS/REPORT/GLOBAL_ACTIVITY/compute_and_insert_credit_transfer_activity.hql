INSERT INTO AGG.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT 
    COMMERCIAL_OFFER_CODE
    , TRANSACTION_TYPE
    , SUB_ACCOUNT
    , TRANSACTION_SIGN
    , SOURCE_PLATFORM
    , SOURCE_DATA
    , SERVED_SERVICE
    , SERVICE_CODE
    , DESTINATION_CODE
    , SERVED_LOCATION
    , MEASUREMENT_UNIT
    , RATED_COUNT
    , RATED_VOLUME
    , TAXED_AMOUNT
    , UNTAXED_AMOUNT
    , INSERT_DATE
    , TRAFFIC_MEAN
    , OPERATOR_CODE
    , TRANSACTION_DATE
FROM(
    SELECT
     COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
    ,'P2P_FEES' TRANSACTION_TYPE
    ,'MAIN' SUB_ACCOUNT
    ,'+' TRANSACTION_SIGN
    ,'P2P' SOURCE_PLATFORM
    , 'FT_CREDIT_TRANSFER' SOURCE_DATA
    , 'P2P_TRAFFIC' SERVED_SERVICE
    , 'NVX_P2P' SERVICE_CODE
    , 'DEST_ND' DESTINATION_CODE
    , NULL SERVED_LOCATION
    , 'HIT' MEASUREMENT_UNIT
    , SUM (1) RATED_COUNT
    , SUM (1) RATED_VOLUME
    , SUM (TRANSFER_FEES) TAXED_AMOUNT
    , SUM ((1-0.1925) * TRANSFER_FEES) UNTAXED_AMOUNT
    , CURRENT_TIMESTAMP INSERT_DATE
    ,'REVENUE' TRAFFIC_MEAN
    , SENDER_OPERATOR_CODE OPERATOR_CODE
    , REFILL_DATE TRANSACTION_DATE
    FROM  MON.FT_CREDIT_TRANSFER
    WHERE REFILL_DATE = '###SLICE_VALUE###' AND TERMINATION_IND = '000'
    GROUP BY
    REFILL_DATE
    , COMMERCIAL_OFFER
    , SENDER_OPERATOR_CODE
    UNION
    SELECT
     COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
    ,'P2P' TRANSACTION_TYPE
    ,'MAIN' SUB_ACCOUNT
    ,'0' TRANSACTION_SIGN
    ,'P2P' SOURCE_PLATFORM
    , 'FT_CREDIT_TRANSFER' SOURCE_DATA
    , 'P2P_TRAFFIC' SERVED_SERVICE
    , 'NVX_P2P_TRANS' SERVICE_CODE
    , 'DEST_ND' DESTINATION_CODE
    , NULL SERVED_LOCATION
    , 'HIT' MEASUREMENT_UNIT
    , SUM (1) RATED_COUNT
    , SUM (1) RATED_VOLUME
    , SUM (TRANSFER_AMT) TAXED_AMOUNT
    , SUM ((1-0.1925) * TRANSFER_AMT) UNTAXED_AMOUNT
    , CURRENT_TIMESTAMP INSERT_DATE
    ,'AIRTIME' TRAFFIC_MEAN
    , SENDER_OPERATOR_CODE OPERATOR_CODE
    , REFILL_DATE TRANSACTION_DATE
    FROM  MON.FT_CREDIT_TRANSFER
    WHERE REFILL_DATE = '###SLICE_VALUE###' AND TERMINATION_IND = '000'
    GROUP BY
    REFILL_DATE
    , COMMERCIAL_OFFER
    , SENDER_OPERATOR_CODE
) A
;
