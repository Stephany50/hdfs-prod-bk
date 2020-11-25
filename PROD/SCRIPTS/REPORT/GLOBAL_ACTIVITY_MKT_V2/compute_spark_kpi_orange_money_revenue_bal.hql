INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG


----------- Stock total client OM
SELECT
     'BALANCE_OM' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , 'UNKNOWN'  SERVICE_CODE
     , 'BALANCE_OM' KPI
     , 'UNKNOWN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(nvl(BALANCE,0)) TOTAL_AMOUNT
     , SUM(nvl(BALANCE,0)) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , ORIGINAL_FILE_DATE TRANSACTION_DATE
     ,'COMPUTE_KPI_OM_BALANCE' JOB_NAME
     , 'IT_OM_ALL_BALANCE' SOURCE_TABLE
FROM CDR.SPARK_IT_OM_ALL_BALANCE B2
LEFT JOIN ( SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
    WHERE EVENT_DATE in (select max(event_date) from MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date>=date_sub('###SLICE_VALUE###',6) )
    GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE
) C ON nvl(C.ACCESS_KEY,'ND') = nvl(B2.account_id,'ND')
left join (
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
) site on  site.msisdn =B2.account_id
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE ORIGINAL_FILE_DATE ='###SLICE_VALUE###' AND USER_CATEGORY IN ('Subscriber')
GROUP BY
    ORIGINAL_FILE_DATE,
    C.PROFILE,
    C.OPERATOR_CODE,
    REGION_ID





