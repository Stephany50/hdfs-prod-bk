    -- Revenu GPRS PAYGO MAIN
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_CELL
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
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND MAIN_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
               , access_key
        UNION
        SELECT
             access_key
             , UPPER(PROFILE) COMMERCIAL_OFFER_CODE
             ,'PROMO' SUB_ACCOUNT
             , SUM (PROMO_CREDIT) TAXED_AMOUNT
             , OPERATOR_CODE OPERATOR_CODE
        FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'
          AND PROMO_CREDIT > 0
        GROUP BY
            DEACTIVATION_DATE
               ,UPPER(PROFILE)
               , OPERATOR_CODE
               , access_key
    ) A
LEFT JOIN (select msisdn, administrative_region from mon.spark_ft_client_last_site_day where event_date='###SLICE_VALUE###') D on d.msisdn=A.access_key
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(d.administrative_region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
group by
COMMERCIAL_OFFER_CODE,
SUB_ACCOUNT,
OPERATOR_CODE,
REGION_ID