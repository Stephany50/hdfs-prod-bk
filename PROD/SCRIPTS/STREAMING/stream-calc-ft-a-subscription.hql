SELECT
   NVL(CONTRACT_TYPE,'NULL'),
   NVL(SPLIT(SUBS.SERVICE_CODE, '\\|')[0],'NULL') MAIN_USAGE_SERVICE_CODE,
   NVL(COMMERCIAL_OFFER,'NULL'),
   NVL(SUBSCRIPTION_SERVICE,'NULL')  SUBS_SERVICE,
   NVL(SUBSCRIPTION_SERVICE_DETAILS,'NULL')  SUBS_BENEFIT_NAME,
   NVL(SUBSCRIPTION_CHANNEL,'NULL') SUBS_CHANNEL,
   NVL(SUBSCRIPTION_RELATED_SERVICE,'NULL') SUBS_RELATED_SERVICE,
   COUNT(SERVED_PARTY_MSISDN) SUBS_TOTAL_COUNT,
   SUM(NVL(RATED_AMOUNT,0)) SUBS_AMOUNT,
   NVL(SERV.SERVICE_CODE, 'NVX_OTH') SERVICE_CODE,
   approx_count_distinct(SERVED_PARTY_MSISDN) MSISDN_COUNT,
   SUM(CASE WHEN NVL(RATED_AMOUNT, 0) > 0 THEN 1 ELSE 0 END) SUBS_EVENT_RATED_COUNT,
   WINDOW.START START_DATE,
   WINDOW.END END_DATE,
   ORIGINAL_FILE_NAME
FROM IN_FT_SUBSCRIPTION SUBS
LEFT JOIN ( SELECT EVENT, MAX(SERVICE_CODE) SERVICE_CODE FROM DIM.DT_SERVICES GROUP BY EVENT) SERV ON (SUBS.SUBSCRIPTION_SERVICE = SERV.EVENT)
GROUP BY
   NVL(CONTRACT_TYPE,'NULL'),
   NVL(SPLIT(SUBS.SERVICE_CODE, '\\|')[0],'NULL'),
   NVL(COMMERCIAL_OFFER,'NULL'),
   NVL(SUBSCRIPTION_SERVICE,'NULL'),
   NVL(SUBSCRIPTION_SERVICE_DETAILS,'NULL'),
   NVL(SUBSCRIPTION_CHANNEL,'NULL'),
   NVL(SERV.SERVICE_CODE, 'NVX_OTH'),
   NVL(SUBSCRIPTION_RELATED_SERVICE,'NULL'),
   WINDOW.START,
   WINDOW.END,
   ORIGINAL_FILE_NAME
