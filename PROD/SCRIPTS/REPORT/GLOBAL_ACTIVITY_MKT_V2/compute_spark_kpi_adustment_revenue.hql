    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V2
SELECT
     'REVENUE_ADJUSTMENT' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , B.GLOBAL_USAGE_CODE  SERVICE_CODE
     , 'REVENUE' KPI
     , IF(ACCT_RES_CODE='1','MAIN','PROMO') SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'IT_ZTE_ADJUSTMENT' SOURCE_TABLE
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(CHARGE/100) TOTAL_AMOUNT
     , SUM(CHARGE/100) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , CREATE_DATE TRANSACTION_DATE
FROM CDR.SPARK_IT_ZTE_ADJUSTMENT A
 LEFT JOIN (SELECT USAGE_CODE, GLOBAL_CODE, GLOBAL_USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE ) B ON B.USAGE_CODE = A.CHANNEL_ID
 LEFT JOIN (
    SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
             LEFT JOIN (SELECT ACCESS_KEY,MAX(EVENT_DATE) MAX_DATE FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
                        WHERE EVENT_DATE between date_sub('###SLICE_VALUE###',7) AND '###SLICE_VALUE###'
                        GROUP BY ACCESS_KEY) B
                       ON B.ACCESS_KEY = A.ACCESS_KEY AND B.MAX_DATE = A.EVENT_DATE
    WHERE B.ACCESS_KEY IS NOT NULL
    GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE ) C ON C.ACCESS_KEY = GET_NNP_MSISDN_9DIGITS(A.ACC_NBR)
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
) site on  site.msisdn =GET_NNP_MSISDN_9DIGITS(A.ACC_NBR)
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE CREATE_DATE = '###SLICE_VALUE###'  AND B.FLUX_SOURCE='ADJUSTMENT' AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37')
  AND CHARGE > 0
GROUP BY
    C.PROFILE
       , B.GLOBAL_CODE
       , IF(ACCT_RES_CODE='1','MAIN','PROMO')
       , B.GLOBAL_CODE
       , B.GLOBAL_USAGE_CODE
       , C.OPERATOR_CODE
       , CREATE_DATE
       ,REGION_ID