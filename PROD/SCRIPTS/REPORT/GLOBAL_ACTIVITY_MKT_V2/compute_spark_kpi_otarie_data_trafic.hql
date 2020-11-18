INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG2
SELECT
    'OTARIE_DATA_USAGE' DESTINATION_CODE
     , COMMERCIAL_OFFER PROFILE_CODE
     , 'UNKNOWN' SERVICE_CODE
     , 'USAGE' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_OTARIE_DATA_TRAFFIC_DAY' SOURCE_TABLE
     , GET_OPERATOR_CODE(A.MSISDN) OPERATOR_CODE
     , SUM(Nbytest)/(1024*1024*1024) TOTAL_AMOUNT
     , SUM(Nbytest)/(1024*1024*1024) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , TRANSACTION_DATE
FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY A
LEFT JOIN (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-11-10'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-11-10'
    group by a.msisdn
) SITE ON  SITE.MSISDN =GET_NNP_MSISDN_9DIGITS(A.MSISDN)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '2020-11-10'
GROUP BY TRANSACTION_DATE, COMMERCIAL_OFFER, GET_OPERATOR_CODE(A.MSISDN), REGION_ID