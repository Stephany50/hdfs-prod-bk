    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V3
SELECT
    'REVENUE_DATA_PAYGO' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) service_code
    ,'REVENUE' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(MAIN_COST) TOTAL_AMOUNT
    ,SUM(MAIN_COST) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,DATECODE
FROM AGG.SPARK_FT_A_GPRS_ACTIVITY a
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE DATECODE ='###SLICE_VALUE###'
AND MAIN_COST>0
GROUP BY
    DATECODE
    ,COMMERCIAL_OFFER
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
    ,OPERATOR_CODE
    ,REGION_ID

UNION ALL
-- Revenu GPRS PAYGO PROMO

SELECT
    'REVENUE_DATA_PAYGO' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) service_code
    ,'REVENUE' KPI
    ,'PROMO' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(PROMO_COST) TOTAL_AMOUNT
    ,SUM(PROMO_COST) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,DATECODE
FROM AGG.SPARK_FT_A_GPRS_ACTIVITY a
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE DATECODE ='###SLICE_VALUE###'
AND PROMO_COST>0
GROUP BY
    DATECODE
    ,COMMERCIAL_OFFER
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
    ,OPERATOR_CODE
    ,REGION_ID
UNION ALL
-- Volume echangé GPRS PAYGO (Usage en Go)

SELECT
    'USAGE_DATA_PAYGO' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) service_code
    ,'USAGE' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(BILLED_UNIT/(1024*1024*1024)) TOTAL_AMOUNT
    ,SUM(BILLED_UNIT/(1024*1024*1024)) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,DATECODE
FROM AGG.SPARK_FT_A_GPRS_ACTIVITY a
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE DATECODE  ='###SLICE_VALUE###'
GROUP BY
    DATECODE
    ,COMMERCIAL_OFFER
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
    ,OPERATOR_CODE
    ,REGION_ID

UNION ALL
SELECT
    'USAGE_DATA_BUNDLE' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) service_code
    ,'USAGE' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(BUCKET_VALUE/(1024*1024*1024)) TOTAL_AMOUNT
    ,SUM(BUCKET_VALUE/(1024*1024*1024)) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,DATECODE
FROM AGG.SPARK_FT_A_GPRS_ACTIVITY a
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE DATECODE ='###SLICE_VALUE###'
GROUP BY
    DATECODE
    ,COMMERCIAL_OFFER
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
    ,OPERATOR_CODE
    ,REGION_ID