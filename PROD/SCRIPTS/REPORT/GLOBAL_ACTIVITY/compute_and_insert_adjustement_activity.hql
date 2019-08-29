INSERT INTO AGG.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
C.PROFILE COMMERCIAL_OFFER_CODE
, B.GLOBAL_CODE TRANSACTION_TYPE
, IF(ACCT_RES_CODE='1','MAIN','PROMO') SUB_ACCOUNT
, '+' TRANSACTION_SIGN
, 'IN' SOURCE_PLATFORM
, 'IT_ZTE_ADJUSTMENT' SOURCE_DATA
, B.GLOBAL_CODE  SERVED_SERVICE
, B.GLOBAL_USAGE_CODE SERVICE_CODE
, 'DEST_ND' DESTINATION_CODE
, NULL SERVED_LOCATION
, NULL MEASUREMENT_UNIT
, SUM(1) RATED_COUNT
, SUM(1) RATED_VOLUME
, SUM(CHARGE/100) TAXED_AMOUNT
, SUM((1-0.1925) * CHARGE / 100 ) UNTAXED_AMOUNT
, CURRENT_TIMESTAMP INSERT_DATE
,'REVENUE' TRAFFIC_MEAN
, C.OPERATOR_CODE OPERATOR_CODE
, NULL LOCATION_CI
, CREATE_DATE TRANSACTION_DATE
FROM CDR.IT_ZTE_ADJUSTMENT A
LEFT JOIN (SELECT USAGE_CODE, GLOBAL_CODE, GLOBAL_USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE ) B ON B.USAGE_CODE = A.CHANNEL_ID
LEFT JOIN (SELECT ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE 
               FROM MON.FT_CONTRACT_SNAPSHOT A
               LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.FT_CONTRACT_SNAPSHOT
                          WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                          GROUP BY ACCESS_KEY) B 
                ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
               WHERE B.ACCESS_KEY IS NOT NULL                 
               GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE ) C
ON C.ACCESS_KEY = GET_NNP_MSISDN_9DIGITS(A.ACC_NBR)
WHERE CREATE_DATE = '###SLICE_VALUE###'  AND B.FLUX_SOURCE='ADJUSTMENT' AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37')
 AND CHARGE > 0
GROUP BY
 C.PROFILE
, B.GLOBAL_CODE
, IF(ACCT_RES_CODE='1','MAIN','PROMO')
, B.GLOBAL_CODE
, B.GLOBAL_USAGE_CODE
, C.OPERATOR_CODE
, CREATE_DATE

;
