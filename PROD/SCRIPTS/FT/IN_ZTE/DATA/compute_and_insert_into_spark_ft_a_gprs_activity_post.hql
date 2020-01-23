INSERT INTO AGG.SPARK_FT_A_GPRS_ACTIVITY_POST PARTITION(DATECODE)
SELECT
    SUBSTR(SESSION_TIME,1,2) TIMECODE
     , SERVED_PARTY_OFFER COMMERCIAL_OFFER
     , SERVICE_CODE SERVICE_CLASS
     , CALL_TYPE TRANSACTION_TYPE
     , UNIT_OF_MEASUREMENT MEASUREMENT_UNIT
     , SUM(NVL(BYTES_RECEIVED,0)) BYTES_RECV
     , SUM(NVL(BYTES_SENT,0)) BYTES_SEND
     --, NULL TOTAL_COMMODITIES
     , SUM(NVL(TOTAL_COST,0)) TOTAL_COST
     , SUM(NVL(BUNDLE_BYTES_USED_VOLUME,0)) BUCKET_VALUE
     , SUM(NVL(SESSION_DURATION,0)) TOTAL_DURATION
     , SUM(NVL((CASE WHEN TOTAL_COST = 0 THEN 0 ELSE SESSION_DURATION END ),0)) BILLED_DURATION
     , COUNT(1) TOTAL_COUNT
     , SUM(NVL((CASE WHEN TOTAL_COST = 0 THEN 0 ELSE 1 END) ,0)) BILLED_COUNT
     , SUM(NVL(TOTAL_UNIT,0)) TOTAL_UNIT
     , SUM(NVL((CASE WHEN TOTAL_COST = 0 THEN 0 ELSE BYTES_RECEIVED + BYTES_SENT END ),0)) BILLED_UNIT
     , OPERATOR_CODE
     , LOCATION_LAC
     , LOCATION_CI
     , SERVICE_CATEGORY
     , ROAMING_INDICATOR
     , SERVED_PARTY_PRICE_PLAN
     , SERVICE_TYPE
     , CONTENT_PROVIDER
     --, NULL TAC_CODE
     , SUM(NVL(MAIN_COST,0)) MAIN_COST
     , SUM(NVL(PROMO_COST,0)) PROMO_COST
     , SDP_GOS_SERV_NAME SERVICE_NAME
     , GPP_USER_LOCATION_INFO
     , CURRENT_TIMESTAMP() INSERT_DATE
     , SESSION_DATE DATECODE
FROM MON.SPARK_FT_CRA_GPRS_POST
WHERE SESSION_DATE = '###SLICE_VALUE###'
GROUP BY SESSION_DATE
       , SUBSTR(SESSION_TIME,1,2)
       , SERVED_PARTY_OFFER
       , SERVICE_CODE
       , CALL_TYPE
       , UNIT_OF_MEASUREMENT
       , OPERATOR_CODE
       , LOCATION_LAC
       , LOCATION_CI
       , SERVICE_CATEGORY
       , ROAMING_INDICATOR
       , SERVED_PARTY_PRICE_PLAN
       , SERVICE_TYPE
       , CONTENT_PROVIDER
       , SDP_GOS_SERV_NAME
       , GPP_USER_LOCATION_INFO
--, NULL
;