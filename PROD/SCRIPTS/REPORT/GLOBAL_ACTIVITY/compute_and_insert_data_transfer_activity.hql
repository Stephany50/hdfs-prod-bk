INSERT INTO REPORT.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
    SENDER_OFFER_PROFILE_CODE COMMERCIAL_OFFER_CODE
   ,'P2P_DATA_TRANSFER' TRANSACTION_TYPE
   ,'MAIN' SUB_ACCOUNT
   ,'-' TRANSACTION_SIGN
   ,'ZTE' SOURCE_PLATFORM
   , 'FT_A_DATA_TRANSFER' SOURCE_DATA
   , 'P2P_DATA_TRANSFER' SERVED_SERVICE
   , 'NVX_P2P_DATA' SERVICE_CODE
   , 'DEST_ND' DESTINATION_CODE
   , NULL SERVED_LOCATION
   , 'HIT' MEASUREMENT_UNIT
   , SUM (1) RATED_COUNT
   , SUM (1) RATED_VOLUME
   , SUM (MONTANT_FRAIS) TAXED_AMOUNT
   , SUM ((1-0.1925) * MONTANT_FRAIS) UNTAXED_AMOUNT
   , CURRENT_TIMESTAMP INSERT_DATE
   ,'REVENUE' TRAFFIC_MEAN
   , SENDER_OPERATOR_CODE OPERATOR_CODE
   , EVENT_DATE TRANSACTION_DATE
FROM   MON.FT_A_DATA_TRANSFER
WHERE EVENT_DATE = '###SLICE_VALUE###' AND MONTANT_FRAIS > 0
GROUP BY
EVENT_DATE
,SENDER_OFFER_PROFILE_CODE
,SENDER_OPERATOR_CODE

