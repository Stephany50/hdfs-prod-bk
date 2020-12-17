

SELECT
    'SNAPSHOT_STOCK_CLIENT' DESTINATION_CODE
    ,'UNKNOWN' PROFILE_CODE
    ,'UNKNOWN' SERVICE_CODE
    ,'SNAPSHOT_STOCK_CLIENT' KPI
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
    where event_date ='###SLIICE_VALUE###' and event_time in (select max(event_time) from cdr.spark_IT_ZEBRA_MASTER_BALANCE where event_date ='###SLIICE_VALUE###') and CATEGORY in ('POS','New POS','Partner POS') AND
    USER_STATUS = 'Y'
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLIICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLIICE_VALUE###'
    group by a.msisdn
) site on a.mobile_number = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REGION_ID,
event_date

union all 
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
    where event_date ='###SLIICE_VALUE###' and event_time in (select max(event_time) from cdr.spark_IT_ZEBRA_MASTER_BALANCE where event_date ='###SLIICE_VALUE###') and CATEGORY in ('In-House Salesman','Independent Salesman') AND
    USER_STATUS = 'Y'
)a
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLIICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLIICE_VALUE###'
    group by a.msisdn
) site on a.mobile_number = site.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
REGION_ID,
event_date