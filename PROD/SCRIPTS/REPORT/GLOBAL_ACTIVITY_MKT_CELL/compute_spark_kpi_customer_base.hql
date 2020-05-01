
insert into AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT
select
    'USER_GROUP' DESTINATION_CODE,
    PROFILE PROFILE_CODE,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT
    ,'FT_GROUP_SUBSCRIBER_SUMMARY' SOURCE_TABLE,
    NVL(b.OPERATOR_CODE,'OCM')OPERATOR_CODE ,
    sum(EFFECTIF) TOTAL_AMOUNT,
    sum(EFFECTIF) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE,
    '' REGION_ID,
    DATE_SUB(event_date,1) TRANSACTION_DATE
from MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY  a
LEFT JOIN DIM.DT_OFFER_PROFILES b on upper(a.PROFILE) = b.PROFILE_CODE
where event_date =DATE_ADD('###SLICE_VALUE###',1) and  statut = 'ACTIF' and CUST_BILLCYCLE in ( 'PURE PREPAID','HYBRID')
group by
    EVENT_DATE,
    profile,
    NVL(b.OPERATOR_CODE,'OCM')

---------------------------------------------
-- Regroupement des 5 INSERT
---------------------------------------------
UNION ALL
SELECT
    CASE
        WHEN x.CUSTOMER_BASE = 'DAILYBASE' THEN 'USER_DAILY_ACTIVE'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN 'USER_30DAYS_WINBACK'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN 'USER_30DAYS_GROUP'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN 'USER_30DAYS_LOST'
        WHEN x.CUSTOMER_BASE = 'CHURN' THEN 'USER_CHURN'
    END DESTINATION_CODE,
    x.formule PROFILE_CODE,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    'FT_ACCOUNT_ACTIVITY' SOURCE_TABLE,
    NVL(b.OPERATOR_CODE,'OCM') OPERATOR_CODE ,
    sum(x.AMOUNT) TOTAL_AMOUNT,
    sum(x.AMOUNT) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE,
    '' REGION_ID,
    x.EVENT_DATE TRANSACTION_DATE
FROM
(
    SELECT a.EVENT_DATE,
           a.formule,
           b.CUSTOMER_BASE,
           CASE
                WHEN b.CUSTOMER_BASE = 'DAILYBASE' THEN DAILYBASE
                WHEN b.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN ALL30DAYSWINBACK
                WHEN b.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN ALL30DAYSBASE
                WHEN b.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN ALL30DAYSLOST
                WHEN b.CUSTOMER_BASE = 'CHURN' THEN CHURN
           END AMOUNT
    FROM MON.SPARK_FT_GROUP_USER_BASE a
    CROSS JOIN
    (
        SELECT 'DAILYBASE' AS CUSTOMER_BASE  UNION ALL
        SELECT 'ALL30DAYSWINBACK' AS CUSTOMER_BASE  UNION ALL
        SELECT 'ALL30DAYSBASE' AS CUSTOMER_BASE  UNION ALL
        SELECT 'ALL30DAYSLOST' AS CUSTOMER_BASE  UNION ALL
        SELECT 'CHURN' AS CUSTOMER_BASE 
    ) b
    where EVENT_DATE='###SLICE_VALUE###'
) x
LEFT JOIN DIM.DT_OFFER_PROFILES b ON  upper(x.formule) = b.PROFILE_CODE
group by x.EVENT_DATE,
    CASE
        WHEN x.CUSTOMER_BASE = 'DAILYBASE' THEN 'USER_DAILY_ACTIVE'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN 'USER_30DAYS_WINBACK'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN 'USER_30DAYS_GROUP'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN 'USER_30DAYS_LOST'
        WHEN x.CUSTOMER_BASE = 'CHURN' THEN 'USER_CHURN'
    END, x.formule, NVL(b.OPERATOR_CODE,'OCM')
UNION ALL
select
    'USER_GROSS_ADD' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT
    ,'FT_A_SUBSCRIBER_SUMMARY' SOURCE_TABLE,
    NVL(OPERATOR_CODE,'OCM')OPERATOR_CODE ,
    sum(TOTAL_ACTIVATION) TOTAL_AMOUNT,
    sum(TOTAL_ACTIVATION) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE,
    '' REGION_ID,
    datecode TRANSACTION_DATE
from AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY a
LEFT JOIN DIM.DT_OFFER_PROFILES b ON  upper(a.COMMERCIAL_OFFER) = b.PROFILE_CODE
WHERE datecode='###SLICE_VALUE###' and NETWORK_DOMAIN = 'GSM'
group by datecode,COMMERCIAL_OFFER,NVL(OPERATOR_CODE,'OCM')
