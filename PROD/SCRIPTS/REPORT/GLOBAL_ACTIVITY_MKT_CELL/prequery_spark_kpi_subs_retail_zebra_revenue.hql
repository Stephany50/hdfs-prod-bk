    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_CELL
SELECT
     'P2P_DATA_VAS' DESTINATION_CODE
     , COMMERCIAL_OFFER PROFILE_CODE
     , 'NVX_VAS_DATA'  SERVICE_CODE
     , 'REVENUE' KPI
     , 'MAIN' SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_SUBS_RETAIL_ZEBRA' SOURCE_TABLE
     , OPERATOR_CODE
     , SUM(MAIN_AMOUNT) TOTAL_AMOUNT
     , SUM(MAIN_AMOUNT) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , TRANSACTION_DATE
FROM MON.SPARK_FT_SUBS_RETAIL_ZEBRA A
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
WHERE TRANSACTION_DATE = '2020-04-29' AND MAIN_AMOUNT > 0
GROUP BY
    TRANSACTION_DATE
       ,COMMERCIAL_OFFER
       ,OPERATOR_CODE
       ,REGION_ID
