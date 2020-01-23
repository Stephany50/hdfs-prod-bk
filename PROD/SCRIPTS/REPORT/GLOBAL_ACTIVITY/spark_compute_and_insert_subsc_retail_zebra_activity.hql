INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
    COMMERCIAL_OFFER COMMERCIAL_OFFER_CODE
     ,'P2P_DATA_VAS' TRANSACTION_TYPE
     ,'MAIN' SUB_ACCOUNT
     ,'-' TRANSACTION_SIGN
     ,'ZTE' SOURCE_PLATFORM
     , 'FT_SUBS_RETAIL_ZEBRA' SOURCE_DATA
     , 'P2P_DATA_VAS' SERVED_SERVICE
     , 'NVX_VAS_DATA' SERVICE_CODE
     , 'DEST_ND' DESTINATION_CODE
     , NULL SERVED_LOCATION
     , 'HIT' MEASUREMENT_UNIT
     , SUM (1) RATED_COUNT
     , SUM (1) RATED_VOLUME
     , SUM (MAIN_AMOUNT) TAXED_AMOUNT
     , SUM ((1-0.1925) * MAIN_AMOUNT) UNTAXED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,'REVENUE' TRAFFIC_MEAN
     , OPERATOR_CODE OPERATOR_CODE
     , NULL LOCATION_CI
     , TRANSACTION_DATE TRANSACTION_DATE
FROM MON.SPARK_FT_SUBS_RETAIL_ZEBRA       --AGG.FT_A_ZTE_ADJUSTMENT_DATA
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND MAIN_AMOUNT > 0
GROUP BY
    TRANSACTION_DATE
       ,COMMERCIAL_OFFER
       ,OPERATOR_CODE

;