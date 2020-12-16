
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
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
where TRANSACTION_DATE  >='2020-12-09'
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
        select * from mon.spark_ft_client_site_traffic_day where event_date>='2020-12-09'
    ) b on a.msisdn = b.msisdn and a.event_date=b.event_date
    where a.event_date>='2020-12-09'
    group by a.msisdn ,a.event_date
) site on a.served_party_msisdn = site.msisdn and a.transaction_date=site.event_date
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
TRANSACTION_DATE
,REGION_ID