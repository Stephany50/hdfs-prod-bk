    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG_NEW
SELECT
    'DEACTIVATED_ACCOUNT_BALANCE' DESTINATION_CODE
     , COMMERCIAL_OFFER_CODE PROFILE_CODE
     , 'NVX_BALANCE' SERVICE_CODE
     , 'REVENUE' KPI
     , SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , OPERATOR_CODE
     , SUM(TAXED_AMOUNT) TOTAL_AMOUNT
     , SUM(TAXED_AMOUNT) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
     ,'COMPUTE_KPI_CONTRACT_SNAPSHOT' JOB_NAME
     , 'FT_CONTRACT_SNAPSHOT' SOURCE_TABLE
     , '###SLICE_VALUE###' TRANSACTION_DATE
FROM(
        SELECT
            access_key
             ,UPPER(PROFILE) COMMERCIAL_OFFER_CODE
             ,'MAIN' SUB_ACCOUNT
             , SUM (MAIN_CREDIT) TAXED_AMOUNT
             , OPERATOR_CODE OPERATOR_CODE
             ,location_ci
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND MAIN_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
               , access_key
               ,location_ci
        UNION
        SELECT
             access_key
             , UPPER(PROFILE) COMMERCIAL_OFFER_CODE
             ,'PROMO' SUB_ACCOUNT
             , SUM (PROMO_CREDIT) TAXED_AMOUNT
             , OPERATOR_CODE OPERATOR_CODE
             ,location_ci
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND PROMO_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
               , access_key
               ,location_ci
 ) A
left join (
    select
        ci location_ci ,
        max(site_name) site_name
    from dim.spark_dt_gsm_cell_code
    group by ci
) b on cast (a.location_ci as int ) = cast (b.location_ci as int )
left join (
    select
        site_name,
        max(administrative_region) administrative_region
    from MON.VW_SDT_CI_INFO_NEW
    group by site_name
) c on upper(trim(b.site_name))=upper(trim(c.site_name))
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(if(c.administrative_region='EXTRÊME-NORD' , 'EXTREME-NORD',c.administrative_region)), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
COMMERCIAL_OFFER_CODE,
SUB_ACCOUNT,
OPERATOR_CODE,
REGION_ID