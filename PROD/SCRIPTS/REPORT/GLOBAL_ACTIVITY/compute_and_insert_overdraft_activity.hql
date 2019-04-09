INSERT INTO REPORT.FT_GLOBAL_ACTIVITY_DAILY PARTITION(TRANSACTION_DATE)
SELECT
UPPER(OFFER_PROFILE_CODE) COMMERCIAL_OFFER_CODE
,'SOS_CREDIT' TRANSACTION_TYPE
,'MAIN' SUB_ACCOUNT
,'-' TRANSACTION_SIGN
, 'ZTE' SOURCE_PLATFORM
,'FT_OVERDRAFT'  SOURCE_DATA
, 'IN_TRAFFIC' SERVED_SERVICE
, 'NVX_SOS' SERVICE_CODE
, 'DEST_ND' DESTINATION_CODE
, NULL SERVED_LOCATION
,'HIT' MEASUREMENT_UNIT
, SUM (1) RATED_COUNT
, SUM (1) RATED_VOLUME
, SUM (FEE) TAXED_AMOUNT
, SUM ((1-0.1925) * FEE) UNTAXED_AMOUNT
, CURRENT_TIMESTAMP INSERT_DATE
,'REVENUE' TRAFFIC_MEAN
, OPERATOR_CODE OPERATOR_CODE
, TRANSACTION_DATE TRANSACTION_DATE
FROM MON.FT_OVERDRAFT
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(FEE_FLAG,'ND') ='YES'
GROUP BY
UPPER(OFFER_PROFILE_CODE)
, OPERATOR_CODE
, TRANSACTION_DATE
;
