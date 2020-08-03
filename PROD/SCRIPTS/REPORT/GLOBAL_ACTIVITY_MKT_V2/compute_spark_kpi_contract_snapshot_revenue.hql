    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V3
SELECT
    'DEACTIVATED_ACCOUNT_BALANCE' DESTINATION_CODE
     , COMMERCIAL_OFFER_CODE PROFILE_CODE
     , 'NVX_BALANCE' SERVICE_CODE
     , 'REVENUE' KPI
     , SUB_ACCOUNT
     , 'HIT' MEASUREMENT_UNIT
     , 'FT_CONTRACT_SNAPSHOT' SOURCE_TABLE
     , OPERATOR_CODE
     , SUM(TAXED_AMOUNT) TOTAL_AMOUNT
     , SUM(TAXED_AMOUNT) RATED_AMOUNT
     , CURRENT_TIMESTAMP INSERT_DATE
     , REGION_ID
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
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) c on a.location_ci = c.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(c.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
COMMERCIAL_OFFER_CODE,
SUB_ACCOUNT,
OPERATOR_CODE,
REGION_ID