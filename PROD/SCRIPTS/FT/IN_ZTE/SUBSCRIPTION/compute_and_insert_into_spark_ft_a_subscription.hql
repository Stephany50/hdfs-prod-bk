INSERT INTO AGG.SPARK_FT_A_SUBSCRIPTION  PARTITION(TRANSACTION_DATE)
SELECT 
    SUBSTR(TRANSACTION_TIME, 1, 2) TRANSACTION_TIME, 
    CONTRACT_TYPE, 
    OPERATOR_CODE, 
    SPLIT(SUBS.SERVICE_CODE, '\\|')[0] MAIN_USAGE_SERVICE_CODE,   
    COMMERCIAL_OFFER,
    PREVIOUS_COMMERCIAL_OFFER,
    SUBSCRIPTION_SERVICE  SUBS_SERVICE,
    SUBSCRIPTION_SERVICE_DETAILS  SUBS_BENEFIT_NAME,
    SUBSCRIPTION_CHANNEL SUBS_CHANNEL,
    SUBSCRIPTION_RELATED_SERVICE SUBS_RELATED_SERVICE,
    COUNT(SERVED_PARTY_MSISDN) SUBS_TOTAL_COUNT,
    SUM(RATED_AMOUNT) SUBS_AMOUNT,
    'IN_ZTE' SOURCE_PLATFORM,
    'FT_SUBSCRIPTION' SOURCE_DATA,
    CURRENT_TIMESTAMP() INSERT_DATE,
    NVL(SERV.SERVICE_CODE, 'NVX_OTH') SERVICE_CODE,
    COUNT(DISTINCT SERVED_PARTY_MSISDN) MSISDN_COUNT,
    SUM(CASE WHEN NVL(RATED_AMOUNT, 0) > 0 THEN 1 ELSE 0 END) SUBS_EVENT_RATED_COUNT,
    RATED_AMOUNT SUBS_PRICE_UNIT,
    TRANSACTION_DATE
FROM MON.SPARK_FT_SUBSCRIPTION SUBS
LEFT JOIN ( SELECT EVENT, MAX(SERVICE_CODE) SERVICE_CODE FROM DIM.DT_SERVICES GROUP BY EVENT) SERV ON (SUBS.SUBSCRIPTION_SERVICE = SERV.EVENT)
WHERE SUBS.TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY 
    SUBSTR(TRANSACTION_TIME, 1, 2),
    CONTRACT_TYPE,
    OPERATOR_CODE,
    SPLIT(SUBS.SERVICE_CODE, '\\|')[0],
    COMMERCIAL_OFFER,
    PREVIOUS_COMMERCIAL_OFFER,
    SUBSCRIPTION_SERVICE,
    SUBSCRIPTION_SERVICE_DETAILS,
    SUBSCRIPTION_CHANNEL,
    SUBSCRIPTION_RELATED_SERVICE,
    NVL(SERV.SERVICE_CODE, 'NVX_OTH'),  
    RATED_AMOUNT,
    TRANSACTION_DATE
;

