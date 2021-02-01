--Insertion du Revenu des souscriptions voix (Orange Bundle)
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
SELECT
'REVENUE_VOICE_BUNDLE' DESTINATION_CODE
,COMMERCIAL_OFFER PROFILE_CODE
,'BUN_VOX' SERVICE_CODE
,'REVENUE' KPI
,'MAIN' SUB_ACCOUNT
,'HIT' MEASUREMENT_UNIT
,OPERATOR_CODE
,SUM(nvl(AMOUNT_SVA,0) + nvl(AMOUNT_VOICE_ONNET,0) +nvl(AMOUNT_VOICE_OFFNET,0)+nvl(AMOUNT_VOICE_INTER,0)+nvl(AMOUNT_VOICE_ROAMING,0)) TOTAL_AMOUNT
,SUM(nvl(AMOUNT_SVA,0) + nvl(AMOUNT_VOICE_ONNET,0) +nvl(AMOUNT_VOICE_OFFNET,0)+nvl(AMOUNT_VOICE_INTER,0)+nvl(AMOUNT_VOICE_ROAMING,0)) RATED_AMOUNT
,CURRENT_TIMESTAMP INSERT_DATE
, REGION_ID
,TRANSACTION_DATE
,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
, 'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION   a
LEFT JOIN (select max(region) region,ci from (select region_territoriale region , ci from DIM.SPARK_DT_GSM_CELL_CODE_MKT ) t group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
AND nvl(AMOUNT_SVA,0) + nvl(AMOUNT_VOICE_ONNET,0) +nvl(AMOUNT_VOICE_OFFNET,0)+nvl(AMOUNT_VOICE_INTER,0)+nvl(AMOUNT_VOICE_ROAMING,0)>0
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID

UNION ALL
      --Insertion du Nombre des souscriptions voix (Usage Orange Bundle)
SELECT
    'USAGE_VOICE_BUNDLE' DESTINATION_CODE
    ,FORMULE PROFILE_CODE
    ,'BUN_VOX' SERVICE_CODE
    ,'USAGE' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(BUNDLE_TEL_DURATION) / 60 TOTAL_AMOUNT
    ,SUM(BUNDLE_TEL_DURATION) / 60 RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,EVENT_DATE TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    , 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE
FROM MON.SPARK_FT_CONSO_MSISDN_DAY   a
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
) site on  site.msisdn =a.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE EVENT_DATE  = '###SLICE_VALUE###'
GROUP BY EVENT_DATE
        ,FORMULE
        ,OPERATOR_CODE
        ,REGION_ID

UNION ALL
--Insertion du Revenu des souscriptions SMS (Orange Bundle)
SELECT
    'REVENUE_SMS_BUNDLE' DESTINATION_CODE
    , COMMERCIAL_OFFER PROFILE_CODE
    , 'BUN_SMS' SERVICE_CODE
    , 'REVENUE' KPI
    , 'MAIN' SUB_ACCOUNT
    , 'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(nvl(AMOUNT_SMS_ONNET,0) +nvl(AMOUNT_SMS_OFFNET,0)+nvl(AMOUNT_SMS_INTER,0)+nvl(AMOUNT_SMS_ROAMING,0)) TOTAL_AMOUNT
    ,SUM(nvl(AMOUNT_SMS_ONNET,0) +nvl(AMOUNT_SMS_OFFNET,0)+nvl(AMOUNT_SMS_INTER,0)+nvl(AMOUNT_SMS_ROAMING,0)) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    , 'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  a
LEFT JOIN (select max(region) region,ci from (select region_territoriale region , ci from DIM.SPARK_DT_GSM_CELL_CODE_MKT ) t group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND  nvl(AMOUNT_SMS_ONNET,0) +nvl(AMOUNT_SMS_OFFNET,0)+nvl(AMOUNT_SMS_INTER,0)+nvl(AMOUNT_SMS_ROAMING,0)>0
    GROUP BY
    TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID

UNION ALL
--Insertion du Nombre de souscriptions SMS (Orange Bundle SMS)
SELECT
    'USAGE_SMS_BUNDLE' DESTINATION_CODE
    ,FORMULE PROFILE_CODE
    , 'BUN_SMS' SERVICE_CODE
    , 'USAGE' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(BUNDLE_SMS_COUNT) TOTAL_AMOUNT
    ,SUM(BUNDLE_SMS_COUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,EVENT_DATE TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    , 'FT_CONSO_MSISDN_DAY' SOURCE_TABLE
FROM MON.SPARK_FT_CONSO_MSISDN_DAY a
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
) site on  site.msisdn =a.msisdn
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(site.administrative_region_b),upper(site.administrative_region_a), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE EVENT_DATE = '###SLICE_VALUE###'
GROUP BY EVENT_DATE
    ,FORMULE
    ,OPERATOR_CODE
    ,REGION_ID


UNION ALL

--Insertion du Revenu des souscriptions Data (2G Bundle)
SELECT
    'REVENUE_DATA_BUNDLE' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    , 'NVX_USS' SERVICE_CODE
    ,'REVENUE' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(amount_data) TOTAL_AMOUNT
    ,SUM(amount_data) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  a
LEFT JOIN (select max(region) region,ci from (select region_territoriale region , ci from DIM.SPARK_DT_GSM_CELL_CODE_MKT ) t group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND  amount_data>0 and upper(trim(subs_benefit_name))!='RP DATA SHAPE_5120K' and  subs_benefit_name  is not null and  subs_channel <>'32'
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID


UNION ALL

--Insertion du Revenu des souscriptions Data (2G Bundle)
SELECT
    'OM_DATA' DESTINATION_CODE
    , COMMERCIAL_OFFER PROFILE_CODE
    , 'NVX_OM_DATA' SERVICE_CODE
    ,'REVENUE' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(amount_data) TOTAL_AMOUNT
    ,SUM(amount_data) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  a
LEFT JOIN (select max(region) region,ci from (select region_territoriale region , ci from DIM.SPARK_DT_GSM_CELL_CODE_MKT ) t group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND  amount_data>0 and upper(trim(subs_benefit_name))!='RP DATA SHAPE_5120K' and  subs_benefit_name  is not null and  subs_channel ='32'
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID

--Insertion du Revenu des others
UNION ALL
SELECT
    'UNKNOWN_BUN' DESTINATION_CODE
    , COMMERCIAL_OFFER PROFILE_CODE
    , 'UNKNOWN_BUN' SERVICE_CODE
    ,'REVENUE' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(SUBS_AMOUNT) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  a
LEFT JOIN (select max(region) region,ci from (select region_territoriale region , ci from DIM.SPARK_DT_GSM_CELL_CODE_MKT ) t group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' and  (upper(trim(subs_benefit_name))='RP DATA SHAPE_5120K' or  subs_benefit_name  is  null or (amount_data+nvl(AMOUNT_SMS_ONNET,0) +nvl(AMOUNT_SMS_OFFNET,0)+nvl(AMOUNT_SMS_INTER,0)+nvl(AMOUNT_SMS_ROAMING,0)+nvl(AMOUNT_SVA,0) + nvl(AMOUNT_VOICE_ONNET,0) +nvl(AMOUNT_VOICE_OFFNET,0)+nvl(AMOUNT_VOICE_INTER,0)+nvl(AMOUNT_VOICE_ROAMING,0)=0)) and SUBS_AMOUNT>0
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID
