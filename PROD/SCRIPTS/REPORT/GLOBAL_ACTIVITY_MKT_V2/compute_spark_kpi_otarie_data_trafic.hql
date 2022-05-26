INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW
SELECT
    'OTARIE_DATA_USAGE' DESTINATION_CODE
     , COMMERCIAL_OFFER PROFILE_CODE
     , 'UNKNOWN' SERVICE_CODE
     , 'USAGE' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , GET_OPERATOR_CODE(A.MSISDN) OPERATOR_CODE
     , SUM(Nbytest)/(1024*1024*1024) TOTAL_AMOUNT
     , SUM(Nbytest)/(1024*1024*1024) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,'COMPUTE_KPI_OTARIE_DATA_TRAFFIC' JOB_NAME
    , 'FT_OTARIE_DATA_TRAFFIC_DAY' SOURCE_TABLE
    , TRANSACTION_DATE
FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY A
LEFT JOIN (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) SITE ON  SITE.MSISDN =GET_NNP_MSISDN_9DIGITS(A.MSISDN)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY TRANSACTION_DATE, COMMERCIAL_OFFER, GET_OPERATOR_CODE(A.MSISDN), REGION_ID

UNION ALL
SELECT
    'OTARIE_DATA_USERS_DAILY_1Mo' DESTINATION_CODE
     , NULL PROFILE_CODE
     , 'UNKNOWN' SERVICE_CODE
     , 'OTARIE_DATA_USERS_DAILY_1Mo' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , GET_OPERATOR_CODE(A.MSISDN) OPERATOR_CODE
     , SUM(CASE WHEN (traffic_daily_up+traffic_daily_down)/1024/1024>=1 THEN 1 ELSE 0 END) TOTAL_AMOUNT
     , SUM(CASE WHEN (traffic_daily_up+traffic_daily_down)/1024/1024>=1 THEN 1 ELSE 0 END)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,'COMPUTE_KPI_OTARIE_DATA_TRAFFIC' JOB_NAME
    , 'FT_OTARIE_USERS_TRAFFIC' SOURCE_TABLE
    , TRANSACTION_DATE
FROM MON.SPARK_FT_OTARIE_USERS_TRAFFIC A
LEFT JOIN (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) SITE ON  SITE.MSISDN =GET_NNP_MSISDN_9DIGITS(A.MSISDN)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY TRANSACTION_DATE, GET_OPERATOR_CODE(A.MSISDN), REGION_ID

UNION ALL
SELECT
    'OTARIE_DATA_USERS_7_DAYS_1Mo' DESTINATION_CODE
     , NULL PROFILE_CODE
     , 'UNKNOWN' SERVICE_CODE
     , 'OTARIE_DATA_USERS_7_DAYS_1Mo' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , GET_OPERATOR_CODE(A.MSISDN) OPERATOR_CODE
     , SUM(CASE WHEN (traffic_7_days_up+traffic_7_days_down)/1024/1024>=1 THEN 1 ELSE 0 END) TOTAL_AMOUNT
     , SUM(CASE WHEN (traffic_7_days_up+traffic_7_days_down)/1024/1024>=1 THEN 1 ELSE 0 END)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,'COMPUTE_KPI_OTARIE_DATA_TRAFFIC' JOB_NAME
    , 'FT_OTARIE_USERS_TRAFFIC' SOURCE_TABLE
    , TRANSACTION_DATE
FROM MON.SPARK_FT_OTARIE_USERS_TRAFFIC A
LEFT JOIN (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) SITE ON  SITE.MSISDN =GET_NNP_MSISDN_9DIGITS(A.MSISDN)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY TRANSACTION_DATE, GET_OPERATOR_CODE(A.MSISDN), REGION_ID



UNION ALL
SELECT
    'OTARIE_DATA_USERS_MTD_1Mo' DESTINATION_CODE
     , NULL PROFILE_CODE
     , 'UNKNOWN' SERVICE_CODE
     , 'OTARIE_DATA_USERS_MTD_1Mo' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , GET_OPERATOR_CODE(A.MSISDN) OPERATOR_CODE
     , SUM(CASE WHEN (traffic_mtd_up+traffic_mtd_down)/1024/1024>=1 THEN 1 ELSE 0 END) TOTAL_AMOUNT
     , SUM(CASE WHEN (traffic_mtd_up+traffic_mtd_down)/1024/1024>=1 THEN 1 ELSE 0 END)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,'COMPUTE_KPI_OTARIE_DATA_TRAFFIC' JOB_NAME
    , 'FT_OTARIE_USERS_TRAFFIC' SOURCE_TABLE
    , TRANSACTION_DATE
FROM MON.SPARK_FT_OTARIE_USERS_TRAFFIC A
LEFT JOIN (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) SITE ON  SITE.MSISDN =GET_NNP_MSISDN_9DIGITS(A.MSISDN)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY TRANSACTION_DATE, GET_OPERATOR_CODE(A.MSISDN), REGION_ID


UNION ALL
SELECT
    'OTARIE_DATA_USERS_30_DAYS_1Mo' DESTINATION_CODE
     , NULL PROFILE_CODE
     , 'UNKNOWN' SERVICE_CODE
     , 'OTARIE_DATA_USERS_30_DAYS_1Mo' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , GET_OPERATOR_CODE(A.MSISDN) OPERATOR_CODE
     , SUM(CASE WHEN (traffic_30_days_up+traffic_30_days_down)/1024/1024>=1 THEN 1 ELSE 0 END) TOTAL_AMOUNT
     , SUM(CASE WHEN (traffic_30_days_up+traffic_30_days_down)/1024/1024>=1 THEN 1 ELSE 0 END)  RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,'COMPUTE_KPI_OTARIE_DATA_TRAFFIC' JOB_NAME
    , 'FT_OTARIE_USERS_TRAFFIC' SOURCE_TABLE
    , TRANSACTION_DATE
FROM MON.SPARK_FT_OTARIE_USERS_TRAFFIC A
LEFT JOIN (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='###SLICE_VALUE###'
    ) b on a.msisdn = b.msisdn
    where a.event_date='###SLICE_VALUE###'
    group by a.msisdn
) SITE ON  SITE.MSISDN =GET_NNP_MSISDN_9DIGITS(A.MSISDN)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY TRANSACTION_DATE, GET_OPERATOR_CODE(A.MSISDN), REGION_ID