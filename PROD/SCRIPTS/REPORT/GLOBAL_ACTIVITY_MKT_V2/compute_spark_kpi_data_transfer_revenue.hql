    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
SELECT
    'P2P_DATA_TRANSFER' DESTINATION_CODE
     , SENDER_OFFER_PROFILE_CODE PROFILE_CODE
     , 'NVX_P2P_DATA' SERVICE_CODE
     , 'REVENUE' KPI
     , 'MAIN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , SENDER_OPERATOR_CODE OPERATOR_CODE
     , SUM (amount_charged) TOTAL_AMOUNT
     , SUM (amount_charged) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,  TRANSACTION_DATE
    ,'COMPUTE_KPI_DATA_TRANSFER' JOB_NAME
     , 'FT_DATA_TRANSFER' SOURCE_TABLE
FROM   MON.SPARK_FT_DATA_TRANSFER A
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
) site on  site.msisdn =GET_NNP_MSISDN_9DIGITS(A.SENDER_MSISDN)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' AND amount_charged > 0
GROUP BY
TRANSACTION_DATE
,SENDER_OFFER_PROFILE_CODE
,SENDER_OPERATOR_CODE
,REGION_ID