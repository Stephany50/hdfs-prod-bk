    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
SELECT
    'SOS_DATA' DESTINATION_CODE
     , UPPER(OFFER_PROFILE_CODE) PROFILE_CODE
     , 'NVX_SOS_DATA' SERVICE_CODE
     , 'REVENUE' KPI
     , 'MAIN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , OPERATOR_CODE
     , SUM (AMOUNT) TOTAL_AMOUNT
     , SUM (AMOUNT) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , TRANSACTION_DATE TRANSACTION_DATE
     ,'COMPUTE_KPI_EMERGENCY_DATA' JOB_NAME
     , 'FT_EMERGENCY_DATA' SOURCE_TABLE
FROM MON.SPARK_FT_EMERGENCY_DATA A
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
) site on  site.msisdn =GET_NNP_MSISDN_9DIGITS(A.msisdn)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND NVL(TRANSACTION_TYPE,'ND') ='LOAN'
GROUP BY
    UPPER(OFFER_PROFILE_CODE)
       , OPERATOR_CODE
       , TRANSACTION_DATE
       , REGION_ID


