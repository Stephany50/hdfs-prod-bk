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
,SUM(SUBS_AMOUNT*coef_voix) TOTAL_AMOUNT
,SUM(SUBS_AMOUNT*coef_voix)  RATED_AMOUNT
,CURRENT_TIMESTAMP INSERT_DATE
, REGION_ID
,TRANSACTION_DATE
,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
, 'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION   ud
left join  (
    select event,
        (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
        (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
        (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
        (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
        nvl(DATA_BUNDLE,0) data
        from dim.dt_services
) events on upper(trim(ud.SUBS_BENEFIT_NAME)) = upper(trim(events.EVENT))
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (ud.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
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
    ,SUM(SUBS_AMOUNT*coef_sms) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT*coef_sms)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    , 'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  ud
left join  (
    select event,
        (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
        (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
        (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
        (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
        nvl(DATA_BUNDLE,0) data
        from dim.dt_services
) events on upper(trim(ud.SUBS_BENEFIT_NAME)) = upper(trim(events.EVENT))
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (ud.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
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
    ,SUM(SUBS_AMOUNT*data) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT*data)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  ud
left join  (
    select event,
        (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
        (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
        (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
        (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
        nvl(DATA_BUNDLE,0) data
        from dim.dt_services
) events on upper(trim(ud.SUBS_BENEFIT_NAME)) = upper(trim(events.EVENT))
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (ud.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND   upper(trim(subs_benefit_name))!='RP DATA SHAPE_5120K' and  subs_benefit_name  is not null and  subs_channel <>'32'
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
    ,SUM(SUBS_AMOUNT*data) TOTAL_AMOUNT
    ,SUM(SUBS_AMOUNT*data)  RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,TRANSACTION_DATE
    ,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
    ,'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION  ud
left join  (
    select event,
        (nvl(VOIX_ONNET,0) + nvl(VOIX_OFFNET,0) + nvl(VOIX_INTER,0)+ nvl(VOIX_ROAMING,0)) coef_voix,
        (nvl(SMS_ONNET,0) +nvl(SMS_OFFNET,0)+nvl(SMS_INTER,0)+nvl(SMS_ROAMING,0)) coef_sms,
        (case when data_bundle != 1 then nvl(DATA_BUNDLE,0) else 0 end) data_combo,
        (case when  data_bundle = 1 then nvl(DATA_BUNDLE,0) else 0 end) data_pur,
        nvl(DATA_BUNDLE,0) data
        from dim.dt_services
) events on upper(trim(ud.SUBS_BENEFIT_NAME)) = upper(trim(events.EVENT))
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (ud.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND  upper(trim(subs_benefit_name))!='RP DATA SHAPE_5120K' and  subs_benefit_name  is not null and  subs_channel ='32'
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
FROM AGG.SPARK_FT_A_SUBSCRIPTION  ud
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (ud.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###' and  (upper(trim(subs_benefit_name))='RP DATA SHAPE_5120K' or  subs_benefit_name  is  null )
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID

-- Insertion du Revenu data Combo
UNION ALL
SELECT
'COMBO_DATA' DESTINATION_CODE
,COMMERCIAL_OFFER PROFILE_CODE
,'COMBO_DATA' SERVICE_CODE
,'REVENUE_COMBO_DATA' KPI
,'MAIN' SUB_ACCOUNT
,'HIT' MEASUREMENT_UNIT
,OPERATOR_CODE
,SUM(NVL(AMOUNT_DATA, 0)) TOTAL_AMOUNT
,SUM(NVL(AMOUNT_DATA, 0)) RATED_AMOUNT
,CURRENT_TIMESTAMP INSERT_DATE
, REGION_ID
,TRANSACTION_DATE
,'COMPUTE_KPI_SUBSCRIPTION_REVENUE' JOB_NAME
, 'FT_A_SUBSCRIPTION' SOURCE_TABLE
FROM AGG.SPARK_FT_A_SUBSCRIPTION   ud
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (ud.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
AND COMBO = "OUI"
GROUP BY TRANSACTION_DATE
    ,COMMERCIAL_OFFER
    ,OPERATOR_CODE
    ,REGION_ID