INSERT INTO  AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
SELECT
     'REVENUE_ADJUSTMENT' DESTINATION_CODE
     , C.PROFILE PROFILE_CODE
     , B.GLOBAL_USAGE_CODE  SERVICE_CODE
     , 'REVENUE' KPI
     , IF(ACCT_RES_CODE='1','MAIN','PROMO') SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , C.OPERATOR_CODE OPERATOR_CODE
     , SUM(CHARGE/100) TOTAL_AMOUNT
     , SUM(CHARGE/100) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     , CREATE_DATE TRANSACTION_DATE
     ,'COMPUTE_KPI_ADJUSTMENT' JOB_NAME
     , 'IT_ZTE_ADJUSTMENT' SOURCE_TABLE
FROM CDR.SPARK_IT_ZTE_ADJUSTMENT A
 LEFT JOIN (SELECT USAGE_CODE, GLOBAL_CODE, GLOBAL_USAGE_CODE, FLUX_SOURCE FROM DIM.DT_ZTE_USAGE_TYPE ) B ON B.USAGE_CODE = A.CHANNEL_ID
 LEFT JOIN (
    -- SELECT A.ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE
    -- FROM MON.SPARK_FT_CONTRACT_SNAPSHOT A
    -- WHERE EVENT_DATE in (select max(event_date) from MON.SPARK_FT_CONTRACT_SNAPSHOT where event_date>=date_sub('###SLICE_VALUE###',6) )
    -- GROUP BY A.ACCESS_KEY, EVENT_DATE, PROFILE

    select ACCESS_KEY, PROFILE, MAX(OPERATOR_CODE) OPERATOR_CODE 
    from MON.SPARK_FT_CONTRACT_SNAPSHOT where EVENT_DATE = '###SLICE_VALUE###'
    group by ACCESS_KEY, PROFILE
) C ON nvl(C.ACCESS_KEY,'ND') = nvl(GET_NNP_MSISDN_9DIGITS(A.ACC_NBR),'ND')
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
) site on  nvl(site.msisdn,'ND') =nvl(GET_NNP_MSISDN_9DIGITS(A.ACC_NBR),'ND')
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE CREATE_DATE = '###SLICE_VALUE###'  AND B.FLUX_SOURCE='ADJUSTMENT' AND CHANNEL_ID IN ('13','9','14','15','26','29','28','37', '109')
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