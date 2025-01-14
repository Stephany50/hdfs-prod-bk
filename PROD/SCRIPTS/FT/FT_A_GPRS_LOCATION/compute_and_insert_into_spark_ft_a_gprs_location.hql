INSERT INTO AGG.SPARK_FT_A_GPRS_LOCATION PARTITION(SESSION_DATE)
SELECT       
    SERVED_PARTY_OFFER, 
    LOCATION_MCC, 
    LOCATION_MNC,
    LOCATION_LAC, 
    LOCATION_CI, 
    SERVICE_TYPE, 
    SERVICE_CODE, 
    CALL_TYPE,
    SERVED_PARTY_TYPE, 
    SUM (MAIN_COST) MAIN_COST,
    SUM (PROMO_COST) PROMO_COST, 
    SUM (TOTAL_COST) TOTAL_COST, 
    SUM (BYTES_SENT) BYTES_SENT,
    SUM (BYTES_RECEIVED) BYTES_RECEIVED, 
    SUM (SESSION_DURATION) SESSION_DURATION, 
    SUM (TOTAL_HITS) TOTAL_HITS,
    SUM (TOTAL_UNIT) TOTAL_UNIT, 
    SUM (BUNDLE_BYTES_USED_VOLUME) BUNDLE_BYTES_USED_VOLUME,
    SUM (MAIN_REMAINING_CREDIT) MAIN_REMAINING_CREDIT, 
    SUM (PROMO_REMAINING_CREDIT) PROMO_REMAINING_CREDIT,
    SUM (BUNDLE_BYTES_REMAINING_VOLUME) BUNDLE_BYTES_REMAINING_VOLUME, 
    SUM (BUNDLE_MMS_REMAINING_VOLUME) BUNDLE_MMS_REMAINING_VOLUME,
    OPERATOR_CODE, 
    CURRENT_TIMESTAMP INSERT_DATE,
    SUM (1) TOTAL_COUNT, 
    SUM (CASE WHEN TOTAL_COST > 0 THEN 1 ELSE 0 END) RATED_COUT, 
    COUNT(DISTINCT SERVED_PARTY_MSISDN) MSISDN_COUNT, 
    ROAMING_INDICATOR,
    TO_DATE(SESSION_DATE)
FROM MON.SPARK_FT_CRA_GPRS A
WHERE SESSION_DATE = '###SLICE_VALUE###'
GROUP BY 
    TO_DATE(SESSION_DATE), SERVED_PARTY_OFFER,
    LOCATION_MCC, LOCATION_MNC,
    LOCATION_LAC, LOCATION_CI,
    SERVICE_TYPE, SERVICE_CODE,
    CALL_TYPE, SERVED_PARTY_TYPE,
    OPERATOR_CODE, ROAMING_INDICATOR