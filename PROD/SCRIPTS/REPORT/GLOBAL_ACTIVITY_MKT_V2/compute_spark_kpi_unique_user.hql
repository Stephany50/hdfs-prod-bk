-- Insertion des Users uniques  Voix et SMS
INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_DG
SELECT
    (CASE
        WHEN DESTINATION='ONNET' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='NVX_SMS' THEN 'USER_SMS_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='NVX_SMS' THEN 'USER_SMS_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INROAM'
        WHEN DESTINATION='ONNET' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INROAM'
    END) DESTINATION_CODE,
    FORMULE PROFILE_CODE,
    'UNIQUE_USERS' SERVICE_CODE,
    'UNIQUE_USERS' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'HIT' MEASUREMENT_UNIT,
     OPERATOR_CODE,
    SUM(USERS_COUNT) TOTAL_AMOUNT,
    SUM(USERS_COUNT) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE ,
    REGION_ID,
    EVENT_DATE TRANSACTION_DATE,
     'COMPUTE_KPI_UNIQUE_USER' JOB_NAME,
    'FT_USERS_DAY' SOURCE_TABLE
FROM
(
    SELECT
        ud.EVENT_DATE,
        ud.FORMULE,
        TRIM(ud.SERVICE) SERVICE,
        TRIM(d.DESTINATION) DESTINATION,
        OPERATOR_CODE,
        (CASE
            WHEN d.DESTINATION='ONNET' THEN ud.ONNET
            WHEN d.DESTINATION='BUNDLE' THEN ud.BUNDLE
            WHEN d.DESTINATION='MTN' THEN ud.MTN
            WHEN d.DESTINATION='NEXTTEL' THEN ud.NEXTTEL
            WHEN d.DESTINATION='CAMTEL' THEN ud.CAMTEL
            WHEN d.DESTINATION='INTERNATIONAL' THEN ud.INTERNATIONAL
            WHEN d.DESTINATION='ROAM' THEN ud.ROAM
            WHEN d.DESTINATION='SET' THEN ud.`SET`
            WHEN d.DESTINATION='INROAM' THEN ud.INROAM
        END) USERS_COUNT,
        REGION_ID
    FROM MON.SPARK_FT_USERS_DAY ud
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
    CROSS JOIN
    (
        SELECT 'ONNET' DESTINATION  UNION ALL
        SELECT 'BUNDLE' DESTINATION  UNION ALL
        SELECT 'MTN' DESTINATION UNION ALL
        SELECT 'NEXTTEL' DESTINATION UNION ALL
        SELECT 'CAMTEL' DESTINATION UNION ALL
        SELECT 'INTERNATIONAL' DESTINATION UNION ALL
        SELECT 'ROAM' DESTINATION UNION ALL
        SELECT 'SET' DESTINATION UNION ALL
        SELECT 'INROAM' DESTINATION
    ) d
    WHERE EVENT_DATE ='###SLICE_VALUE###'
        AND TRIM(SERVICE) <> 'ALL'
)T
WHERE DESTINATION <> 'SET'
GROUP BY
    EVENT_DATE,
    OPERATOR_CODE,
    (CASE
        WHEN DESTINATION='ONNET' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='NVX_SMS' THEN 'USER_SMS_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='NVX_SMS' THEN 'USER_SMS_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='NVX_SMS' THEN 'USER_SMS_INROAM'
        WHEN DESTINATION='ONNET' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ONNET'
        WHEN DESTINATION='BUNDLE' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_BUNDLE'
        WHEN DESTINATION='MTN' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_MTN'
        WHEN DESTINATION='NEXTTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_NEXTTEL'
        WHEN DESTINATION='CAMTEL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_CAMTEL'
        WHEN DESTINATION='INTERNATIONAL' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INTERNATIONAL'
        WHEN DESTINATION='ROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_ROAM'
        WHEN DESTINATION='INROAM' AND SERVICE='VOI_VOX' THEN 'USER_VOICE_INROAM'
    END) ,
    FORMULE,
    region_id

UNION ALL

-- Insertion des Users uniques  DATA
SELECT
    'UNIQUE_DATA_USERS' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'UNIQUE_DATA_USERS' SERVICE_CODE,
    'UNIQUE_DATA_USERS' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'UNKNOWN' MEASUREMENT_UNIT,
     OPERATOR_CODE,
    SUM(rated_count) TOTAL_AMOUNT,
    SUM(rated_count) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    EVENT_DATE TRANSACTION_DATE,
    'COMPUTE_KPI_UNIQUE_USER' JOB_NAME,
    'FT_USERS_DATA_DAY' SOURCE_TABLE
FROM MON.SPARK_FT_USERS_DATA_DAY ud
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
WHERE EVENT_DATE ='###SLICE_VALUE###'

GROUP BY EVENT_DATE, COMMERCIAL_OFFER, OPERATOR_CODE,region_id

UNION ALL
-- Insertion des Users uniques  DATA
SELECT
    'UNIQUE_DATA_USERS_30Jrs' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'UNIQUE_DATA_USERS_30Jrs' SERVICE_CODE,
    'UNIQUE_DATA_USERS_30Jrs' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'UNKNOWN' MEASUREMENT_UNIT,
     OPERATOR_CODE,
    SUM(rated_count_30_days) TOTAL_AMOUNT,
    SUM(rated_count_30_days) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    EVENT_DATE TRANSACTION_DATE,
    'COMPUTE_KPI_UNIQUE_USER' JOB_NAME,
    'FT_USERS_DATA_DAY' SOURCE_TABLE
FROM MON.SPARK_FT_USERS_DATA_DAY ud
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
WHERE EVENT_DATE ='###SLICE_VALUE###'

GROUP BY EVENT_DATE, COMMERCIAL_OFFER, OPERATOR_CODE,region_id


UNION ALL
-- Insertion des Users uniques  DATA
SELECT
    'UNIQUE_DATA_USERS_1Mo_30Jrs' DESTINATION_CODE,
    COMMERCIAL_OFFER PROFILE_CODE,
    'UNIQUE_DATA_USERS_1Mo_30Jrs' SERVICE_CODE,
    'UNIQUE_DATA_USERS_1Mo_30Jrs' KPI,
    'UNKNOWN' SUB_ACCOUNT,
    'UNKNOWN' MEASUREMENT_UNIT,
     OPERATOR_CODE,
    SUM(rated_count_30_days_1mo) TOTAL_AMOUNT,
    SUM(rated_count_30_days_1mo) RATED_AMOUNT,
    CURRENT_TIMESTAMP INSERT_DATE,
    REGION_ID,
    EVENT_DATE TRANSACTION_DATE,
    'COMPUTE_KPI_UNIQUE_USER' JOB_NAME,
    'FT_USERS_DATA_DAY' SOURCE_TABLE
FROM MON.SPARK_FT_USERS_DATA_DAY ud
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
WHERE EVENT_DATE ='###SLICE_VALUE###'

GROUP BY EVENT_DATE, COMMERCIAL_OFFER, OPERATOR_CODE,region_id