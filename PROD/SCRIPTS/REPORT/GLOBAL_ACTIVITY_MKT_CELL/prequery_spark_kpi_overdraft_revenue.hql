    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_CELL
SELECT
    'SOS_CREDIT' DESTINATION_CODE
     , UPPER(OFFER_PROFILE_CODE) PROFILE_CODE
     , 'NVX_SOS' SERVICE_CODE
     , 'REVENUE' KPI
     , 'MAIN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_OVERDRAFT' SOURCE_TABLE
     ,  OPERATOR_CODE
     , SUM (FEE) TOTAL_AMOUNT
     , SUM (FEE) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,  TRANSACTION_DATE
FROM   MON.SPARK_FT_OVERDRAFT A
left join (
    select
        a.msisdn,
        max(a.administrative_region) administrative_region_a,
        max(b.administrative_region) administrative_region_b
    from mon.spark_ft_client_last_site_day a
    left join (
        select * from mon.spark_ft_client_site_traffic_day where event_date='2020-04-29'
    ) b on a.msisdn = b.msisdn
    where a.event_date='2020-04-29'
    group by a.msisdn
) site on  site.msisdn =GET_NNP_MSISDN_9DIGITS(A.served_party_msisdn)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '2020-04-29' AND NVL(FEE_FLAG,'ND') ='YES'
GROUP BY
TRANSACTION_DATE
,UPPER(OFFER_PROFILE_CODE)
,OPERATOR_CODE
,REGION_ID
