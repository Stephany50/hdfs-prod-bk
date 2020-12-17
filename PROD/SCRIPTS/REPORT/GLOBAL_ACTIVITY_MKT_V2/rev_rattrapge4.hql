
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG8
SELECT
    NULL DESTINATION_CODE
     , NULL PROFILE_CODE
     , NULL   SERVICE_CODE
     , 'DATA_VIA_OM' KPI
     , null SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , NULL OPERATOR_CODE
     , SUM (rated_amount) TOTAL_AMOUNT
     , SUM (rated_amount) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     ,  REGION_ID
     , TRANSACTION_DATE
     ,'COMPUTE_KPI_REFILL_TRAFFIC' JOB_NAME
     , 'FT_SUBSCRIPTION' SOURCE_TABLE
FROM  (
SELECT * FROM MON.SPARK_ft_subscription
where TRANSACTION_DATE  between '2019-11-15' and '2019-11-26'
and rated_amount>0
and subscription_channel = '32'
)a
left join (
    select
        a.event_date,
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date between '2019-11-15' and '2019-11-26'
    ) b on a.msisdn = b.msisdn and a.event_date=b.event_date
    where a.event_date between '2019-11-15' and '2019-11-26'
    group by a.msisdn,a.event_date
) site on a.served_party_msisdn = site.msisdn and a.transaction_date=site.event_date
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
TRANSACTION_DATE
,REGION_ID

 select
        NULL region_administrative,
        NULL region_commerciale,
        'Distribution' category,
        'Self Top UP ratio (%)' KPI ,
        'Self Top UP ratio (%)' axe_vue_transversale ,
        null axe_revenu,
        null axe_subscriber,
        concat(a.source_table,'&',b.source_table) source_table,
        'MOY' cummulable,
        (a.rated_amount/b.rated_amount)*100 valeur
    from (
        select sum(rated_amount) rated_amount ,max(source_table) source_table from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG where transaction_date ='2019-11-15'   and KPI in ('REFILL_SELF_TOP','DATA_VIA_OM')
    ) a
    left join (
        select  sum(rated_amount) rated_amount,max(source_table) source_table   from AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG where transaction_date ='2019-11-15'   and KPI= 'VALEUR_AIRTIME'
     )  b



SELECT
    'SNAPSHOT_STOCK_DIST' DESTINATION_CODE
    ,'UNKNOWN' PROFILE_CODE
    ,'UNKNOWN' SERVICE_CODE
    ,'SNAPSHOT_STOCK_DIST' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'UNKNOWN' OPERATOR_CODE
    , sum(available_balance/100)  TOTAL_AMOUNT
    , sum(available_balance/100)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,event_date TRANSACTION_DATE
    ,'COMPUTE_KPI_REFILL_TRAFFIC' JOB_NAME
    , 'IT_ZEBRA_MASTER_BALANCE' SOURCE_TABLE

FROM (
    SELECT
    *
    FROM
    CDR.SPARK_IT_ZEBRA_MASTER_BALANCE
    where event_date ='2019-11-27' and event_time in (select max(event_time) from cdr.spark_IT_ZEBRA_MASTER_BALANCE where event_date ='2019-11-27') and CATEGORY in ('In-House Salesman','Independent Salesman') AND
    USER_STATUS = 'Y'
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2019-11-27'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2019-11-27'
    group by a.msisdn
) site on a.mobile_number = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REGION_ID,
event_date



INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
-- Insertion des Users uniques  DATA
SELECT
    'UNIQUE_DATA_USERS_1Mo_30Jrs' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'UNIQUE_DATA_USERS_1Mo_30Jrs' SERVICE_CODE,
    'UNIQUE_DATA_USERS_1Mo_30Jrs' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'UNKNOWN' MEASUREMENT_UNIT,
     OPERATOR_CODE,
    SUM(rated_count_30_days_1mo) TOTAL_AMOUNT,
    SUM(rated_count_30_days_1mo) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    EVENT_DATE TRANSACTION_DATE,
    'COMPUTE_KPI_UNIQUE_USER' JOB_NAME,
    'FT_USERS_DATA_DAY' SOURCE_TABLE
FROM MON.SPARK_FT_USERS_DATA_DAY b
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) c on b.location_ci = c.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(c.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE EVENT_DATE <='2020-10-31'
GROUP BY EVENT_DATE, COMMERCIAL_OFFER, OPERATOR_CODE,region_id