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
     UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
   ,'GPRS' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'+' TRANSACTION_SIGN
   ,'VOLUBILL' SOURCE_PLATFORM
   ,'FT_A_GPRS_ACTIVITY' SOURCE_DATA
   ,'GPRS_TRAFFIC' SERVED_SERVICE
   , IF(SERVICE_NAME IS NOT NULL, 'NVX_GPRS_SVA', 'NVX_GPRS_PAYGO') SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   , MEASUREMENT_UNIT MEASUREMENT_UNIT
   , SUM(1) RATED_COUNT
   , SUM(TOTAL_UNIT) RATED_VOLUME
   , SUM(MAIN_COST) TAXED_AMOUNT
   , SUM ((1-0.1925) * MAIN_COST) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , OPERATOR_CODE OPERATOR_CODE
   , DATECODE TRANSACTION_DATE
    FROM  AGG.FT_A_GPRS_ACTIVITY
    WHERE DATECODE = '###SLICE_VALUE###' AND MAIN_COST > 0
    GROUP BY
    DATECODE
   ,UPPER(COMMERCIAL_OFFER)
   , IF(SERVICE_NAME IS NOT NULL, 'NVX_GPRS_SVA', 'NVX_GPRS_PAYGO')
   , MEASUREMENT_UNIT
   , OPERATOR_CODE
    UNION
    SELECT
     UPPER(COMMERCIAL_OFFER) COMMERCIAL_OFFER_CODE
   ,'GPRS' TRANSACTION_TYPE
   ,'PROMO' SUB_ACCOUNT
   ,'+' TRANSACTION_SIGN
   ,'VOLUBILL' SOURCE_PLATFORM
   ,'FT_A_GPRS_ACTIVITY' SOURCE_DATA
   ,'GPRS_TRAFFIC' SERVED_SERVICE
   , IF(SERVICE_NAME IS NOT NULL, 'NVX_GPRS_SVA', 'NVX_GPRS_PAYGO') SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   , MEASUREMENT_UNIT MEASUREMENT_UNIT
   , SUM(1) RATED_COUNT
   , SUM(TOTAL_UNIT) RATED_VOLUME
   , SUM(PROMO_COST) TAXED_AMOUNT
   , SUM ((1-0.1925) * PROMO_COST) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , OPERATOR_CODE OPERATOR_CODE
   , DATECODE TRANSACTION_DATE
    FROM  AGG.FT_A_GPRS_ACTIVITY
    WHERE DATECODE = '###SLICE_VALUE###' AND PROMO_COST > 0
    GROUP BY  
    DATECODE
   ,UPPER(COMMERCIAL_OFFER)
   , IF(SERVICE_NAME IS NOT NULL, 'NVX_GPRS_SVA', 'NVX_GPRS_PAYGO')
   , MEASUREMENT_UNIT
   , OPERATOR_CODE
) A

;
