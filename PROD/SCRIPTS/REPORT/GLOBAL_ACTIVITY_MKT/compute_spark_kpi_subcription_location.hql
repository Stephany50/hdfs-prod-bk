INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT
SELECT
    'REVENUE_LOCATION_SUBSCRIPTION' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'MAIN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
    'FT_SUBSCRIPTION_SITE_DAY' SOURCE_TABLE,
    OPERATOR_CODE,
    SUM(RATED_AMOUNT) TOTAL_AMOUNT,
    SUM(RATED_AMOUNT) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE,
    r.REGION_ID,
    EVENT_DATE TRANSACTION_DATE
FROM MON.SPARK_FT_SUBSCRIPTION_SITE_DAY ul
LEFT JOIN DIM.DT_REGIONS_MKT r on TRIM(COALESCE(ul.ADMINISTRATIVE_REGION, 'INCONNU')) = r.ADMINISTRATIVE_REGION
WHERE ul.EVENT_DATE  = '###SLICE_VALUE###'
GROUP BY EVENT_DATE,
    COMMERCIAL_OFFER,
    OPERATOR_CODE,
    r.REGION_ID