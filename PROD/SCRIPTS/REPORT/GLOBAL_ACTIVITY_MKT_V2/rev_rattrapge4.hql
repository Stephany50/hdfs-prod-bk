SELECT
    'REVENUE_DATA_PAYGO' DESTINATION_CODE
    ,COMMERCIAL_OFFER PROFILE_CODE
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END) service_code
    ,'REVENUE' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    ,OPERATOR_CODE
    ,SUM(MAIN_COST) TOTAL_AMOUNT
    ,SUM(MAIN_COST) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,DATECODE
    ,'COMPUTE_KPI_GPRS' JOB_NAME
    ,'FT_A_GPRS_ACTIVITY' SOURCE_TABLE
FROM AGG.SPARK_FT_A_GPRS_ACTIVITY ud
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
WHERE DATECODE ='2021-01-01' and ROAMING_INDICATOR=1
AND MAIN_COST>0
GROUP BY
    DATECODE
    ,COMMERCIAL_OFFER
    ,(CASE WHEN SERVICE_NAME IS NOT NULL THEN 'NVX_GPRS_SVA'  WHEN ROAMING_INDICATOR =1 THEN 'NVX_GPRS_ROAMING' ELSE 'NVX_GPRS_PAYGO' END)
    ,OPERATOR_CODE
    ,REGION_ID