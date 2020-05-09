INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT_V2
SELECT
    (CASE
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
        ELSE IF(SERVICE_CODE= 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER')
    END) DESTINATION_CODE
    ,OFFER_PROFILE_CODE PROFILE_CODE
    , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE) SERVICE_CODE
    ,'REVENUE' KPI
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(MAIN_RATED_AMOUNT) TOTAL_AMOUNT
    ,SUM(MAIN_RATED_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY a
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY
    TRANSACTION_DATE
    ,(CASE
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
        ELSE IF(SERVICE_CODE= 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER')
    END)
    ,OFFER_PROFILE_CODE
    ,OPERATOR_CODE
    , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE)
    ,region_id


UNION ALL

--Insertion du revenue PROMO
SELECT
    (CASE
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
        ELSE IF(SERVICE_CODE= 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER')
    END) DESTINATION_CODE
    ,OFFER_PROFILE_CODE PROFILE_CODE
    , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE) SERVICE_CODE    
    ,'REVENUE' KPI
    ,'PROMO' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(PROMO_RATED_AMOUNT) TOTAL_AMOUNT
    ,SUM(PROMO_RATED_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY a
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY
    TRANSACTION_DATE
    ,(CASE
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'REVENUE_VOICE_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_SMS_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'REVENUE_SMS_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_SMS_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_SMS_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_VOICE_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'REVENUE_VOICE_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'REVENUE_VOICE_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'REVENUE_SMS_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_SMS_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'REVENUE_VOICE_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'REVENUE_SMS_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'REVENUE_VOICE_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'REVENUE_VOICE_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'REVENUE_VOICE_ONNET'
        ELSE IF(SERVICE_CODE= 'VOI_VOX', 'REVENUE_VOICE_OTHER', 'REVENUE_SMS_OTHER')
    END)
    ,OFFER_PROFILE_CODE
    ,OPERATOR_CODE
    , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE)
    ,region_id

UNION ALL
--Insertion des usages Voix en Minute et SMS en Nombre

SELECT
    (CASE
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'USAGE_VOICE_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_SMS_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_SMS_ONNET'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'USAGE_SMS_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_SMS_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'USAGE_VOICE_ROAMING'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_SMS_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_VOICE_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_VOICE_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_VOICE_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'USAGE_SMS_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_SMS_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'USAGE_VOICE_VAS'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'USAGE_VOICE_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_SMS_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_VOICE_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_VOICE_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_VOICE_ONNET'
        ELSE IF(SERVICE_CODE= 'VOI_VOX', 'USAGE_VOICE_OTHER', 'USAGE_SMS_OTHER')
    END) DESTINATION_CODE
    ,OFFER_PROFILE_CODE PROFILE_CODE
    , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE) SERVICE_CODE
    ,'USAGE' KPI
    ,'UNKNOWN' SUB_ACCOUNT
    ,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
           WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
      ) MEASUREMENT_UNIT
    , 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN TOTAL_COUNT
              WHEN SERVICE_CODE='VOI_VOX' THEN DURATION/60 ELSE 0 END) TOTAL_AMOUNT
    ,SUM(CASE WHEN SERVICE_CODE='NVX_SMS' THEN RATED_TOTAL_COUNT
              WHEN SERVICE_CODE='VOI_VOX' THEN RATED_DURATION/60 ELSE 0 END) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    , REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY a
LEFT JOIN (select max(region) region,ci from dim.dt_gsm_cell_code group by CI) b on a.location_ci = b.ci
LEFT JOIN DIM.DT_REGIONS_MKT r ON TRIM(COALESCE(upper(b.region), 'INCONNU')) = upper(r.ADMINISTRATIVE_REGION)
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
GROUP BY
    TRANSACTION_DATE
    ,(CASE
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_INT' THEN 'USAGE_VOICE_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_SMS_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_SMS_ONNET'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_SVA' THEN 'USAGE_SMS_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_SMS_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MT' THEN 'USAGE_VOICE_ROAMING'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_SMS_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_VOICE_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_OCM' THEN 'USAGE_VOICE_ONNET'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_CAM' THEN 'USAGE_VOICE_CAMTEL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_INT' THEN 'USAGE_SMS_INTERNATIONAL'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_SMS_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_SVA' THEN 'USAGE_VOICE_VAS'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_CCSVA' THEN 'USAGE_VOICE_VAS'
        WHEN SERVICE_CODE='NVX_SMS' AND DESTINATION='OUT_NAT_MOB_NEX' THEN 'USAGE_SMS_NEXTTEL'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_ROAM_MO' THEN 'USAGE_VOICE_ROAMING'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MTN' THEN 'USAGE_VOICE_MTN'
        WHEN SERVICE_CODE='VOI_VOX' AND DESTINATION='OUT_NAT_MOB_MVO' THEN 'USAGE_VOICE_ONNET'
        ELSE IF(SERVICE_CODE= 'VOI_VOX', 'USAGE_VOICE_OTHER', 'USAGE_SMS_OTHER')
    END)
    ,OFFER_PROFILE_CODE
    ,(CASE WHEN SERVICE_CODE='NVX_SMS' THEN 'HIT'
           WHEN SERVICE_CODE='VOI_VOX' THEN 'DURATION' ELSE 'UNKOWN' END
      )
    ,OPERATOR_CODE
    , IF(SERVICE_CODE='NVX_USS','NVX_SMS',SERVICE_CODE)
    ,region_id