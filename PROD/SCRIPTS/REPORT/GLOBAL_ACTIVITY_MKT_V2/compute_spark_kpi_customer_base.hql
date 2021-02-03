insert into AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
select
    'USER_GROUP' DESTINATION_CODE,
    PROFILE PROFILE_CODE,
    'UNKNOWN'  SERVICE_CODE,
    'PARC' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    NVL(prof.OPERATOR_CODE,'OCM')OPERATOR_CODE ,
    sum(EFFECTIF) TOTAL_AMOUNT,
    sum(EFFECTIF) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    DATE_SUB(event_date,1) TRANSACTION_DATE
    ,'COMPUTE_KPI_CUSTOMER_BASE' JOB_NAME
    ,'FT_GROUP_SUBSCRIBER_SUMMARY' SOURCE_TABLE
from MON.SPARK_FT_GROUP_SUBSCRIBER_SUMMARY  a
LEFT JOIN DIM.DT_OFFER_PROFILES prof on upper(a.PROFILE) = prof.PROFILE_CODE
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (a.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
where event_date =DATE_ADD('###SLICE_VALUE###',1) and prof.operator_code <> 'SET'
 AND (CASE
        WHEN PROFILE IN ('PREPAID PERSO', 'POSTPAID PERSONNELOCM') THEN
            1
        ELSE 0
      END) = 0
group by
    EVENT_DATE,
    profile,
    NVL(prof.OPERATOR_CODE,'OCM'),
    region_id

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
    where EVENT_DATE='###SLICE_VALUE###'
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
UNION ALL
select
    'USER_GROSS_ADD' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'UNKNOWN'  SERVICE_CODE,
    'PARC' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    NVL(OPERATOR_CODE,'OCM')OPERATOR_CODE ,
    sum(TOTAL_ACTIVATION) TOTAL_AMOUNT,
    sum(TOTAL_ACTIVATION) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    datecode TRANSACTION_DATE
    ,'COMPUTE_KPI_CUSTOMER_BASE' JOB_NAME
    ,'FT_A_SUBSCRIBER_SUMMARY' SOURCE_TABLE
from AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY a
LEFT JOIN DIM.DT_OFFER_PROFILES prof ON  upper(a.COMMERCIAL_OFFER) = prof.PROFILE_CODE
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (a.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE datecode='###SLICE_VALUE###' and NETWORK_DOMAIN = 'GSM'
group by datecode,COMMERCIAL_OFFER,NVL(OPERATOR_CODE,'OCM'),REGION_ID
