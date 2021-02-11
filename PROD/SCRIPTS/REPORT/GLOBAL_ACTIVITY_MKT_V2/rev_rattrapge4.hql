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
    segmentation,
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
    where EVENT_DATE='2021-02-01'
) x
LEFT JOIN DIM.DT_OFFER_PROFILES b ON  upper(x.formule) = b.PROFILE_CODE
group by x.EVENT_DATE,
    CASE
        WHEN x.CUSTOMER_BASE = 'DAILYBASE' THEN 'USER_DAILY_ACTIVE'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN 'USER_30DAYS_WINBACK'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN 'USER_30DAYS_GROUP'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN 'USER_30DAYS_LOST'
        WHEN x.CUSTOMER_BASE = 'CHURN' THEN 'USER_CHURN'
    END, x.formule, NVL(b.OPERATOR_CODE,'OCM'),segmentation
    
    
select
 sum(valeur )*1.1925*1.02*0.95 valeur
from  tmp.budget_sortant2  where  event_date ="2021-02-08"
  
SELECT
    CASE
        WHEN x.CUSTOMER_BASE = 'DAILYBASE' THEN 'USER_DAILY_ACTIVE'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN 'USER_30DAYS_WINBACK'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN 'USER_30DAYS_GROUP'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN 'USER_30DAYS_LOST'
        WHEN x.CUSTOMER_BASE = 'CHURN' THEN 'USER_CHURN'
    END DESTINATION_CODE,
    x.formule PROFILE_CODE,
    'UNKNOWN'  SERVICE_CODE,
    'PARC' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    NVL(prof.OPERATOR_CODE,'OCM') OPERATOR_CODE ,
    sum(x.AMOUNT) TOTAL_AMOUNT,
    sum(x.AMOUNT) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    x.EVENT_DATE TRANSACTION_DATE
    ,'COMPUTE_KPI_CUSTOMER_BASE' JOB_NAME
    ,'FT_ACCOUNT_ACTIVITY' SOURCE_TABLE
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
           END AMOUNT,
           location_ci
    FROM MON.SPARK_FT_GROUP_USER_BASE a
    CROSS JOIN
    (
        SELECT 'DAILYBASE' AS CUSTOMER_BASE  UNION ALL
        SELECT 'ALL30DAYSWINBACK' AS CUSTOMER_BASE  UNION ALL
        SELECT 'ALL30DAYSBASE' AS CUSTOMER_BASE  UNION ALL
        SELECT 'ALL30DAYSLOST' AS CUSTOMER_BASE  UNION ALL
        SELECT 'CHURN' AS CUSTOMER_BASE 
    ) b
    where EVENT_DATE='2021-02-01'
) x
LEFT JOIN DIM.DT_OFFER_PROFILES prof ON  upper(x.formule) = prof.PROFILE_CODE
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (x.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by x.EVENT_DATE,
    CASE
        WHEN x.CUSTOMER_BASE = 'DAILYBASE' THEN 'USER_DAILY_ACTIVE'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSWINBACK' THEN 'USER_30DAYS_WINBACK'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSBASE' THEN 'USER_30DAYS_GROUP'
        WHEN x.CUSTOMER_BASE = 'ALL30DAYSLOST' THEN 'USER_30DAYS_LOST'
        WHEN x.CUSTOMER_BASE = 'CHURN' THEN 'USER_CHURN'
    END, x.formule, NVL(prof.OPERATOR_CODE,'OCM'),region_id


    select
        b.administrative_region region_administrative,
        b.commercial_region region_commerciale,
        'Subscriber overview' category,
        'Gross Adds' KPI ,
        'Gross Adds' axe_vue_transversale ,
        null axe_revenu,
        'GROSS ADDS' axe_subscriber,
        source_table,
        'SUM' cummulable,
        cast(sum(rated_amount) as bigint) valeur
    from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG a
    left join dim.spark_dt_regions_mkt_v2 b on a.region_id = b.region_id
    where transaction_date ='2021-01-01'   and KPI='PARC' and DESTINATION_CODE = 'USER_GROSS_ADD_SUBSCRIPTION'
    group by
    b.administrative_region ,
    b.commercial_region,
    source_table
