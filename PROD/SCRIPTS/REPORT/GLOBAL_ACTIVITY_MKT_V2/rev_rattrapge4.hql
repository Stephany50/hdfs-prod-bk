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
    COMBO,
    SUM(AMOUNT_SVA) AMOUNT_SVA,
    SUM(AMOUNT_VOICE_ONNET) AMOUNT_VOICE_ONNET,
    SUM(AMOUNT_VOICE_OFFNET) AMOUNT_VOICE_OFFNET,
    SUM(AMOUNT_VOICE_INTER) AMOUNT_VOICE_INTER,
    SUM(AMOUNT_VOICE_ROAMING) AMOUNT_VOICE_ROAMING,
    SUM(AMOUNT_SMS_ONNET) AMOUNT_SMS_ONNET,
    SUM(AMOUNT_SMS_OFFNET) AMOUNT_SMS_OFFNET,
    SUM(AMOUNT_SMS_INTER) AMOUNT_SMS_INTER,
    SUM(AMOUNT_SMS_ROAMING) AMOUNT_SMS_ROAMING,
    SUM(AMOUNT_DATA) AMOUNT_DATA,
    nvl(location_ci_b,location_ci_a) LOCATION_CI,
    TRANSACTION_DATE
FROM (
        SELECT subs.* , location_ci_a,location_ci_b
        FROM MON.SPARK_FT_SUBSCRIPTION  subs

        LEFT JOIN (


            select
                a.msisdn,
                max(a.location_ci) location_ci_a,
                max(b.location_ci) location_ci_b,
                max(b.administrative_region) administrative_region_a,
                max(b.administrative_region) administrative_region_b
            from mon.spark_ft_client_last_site_day a
            left join (
                select * from mon.spark_ft_client_site_traffic_day where event_date='2021-01-10'
            ) b on a.msisdn = b.msisdn
            where a.event_date='2021-01-10'
            group by a.msisdn

        ) D on d.msisdn=subs.SERVED_PARTY_MSISDN
        WHERE subs.TRANSACTION_DATE = '2021-01-10'

) SUBS
LEFT JOIN ( SELECT EVENT, MAX(SERVICE_CODE) SERVICE_CODE FROM DIM.DT_SERVICES GROUP BY EVENT) SERV ON (SUBS.SUBSCRIPTION_SERVICE = SERV.EVENT)

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
    TRANSACTION_DATE,
    COMBO,
    nvl(location_ci_b,location_ci_a)

