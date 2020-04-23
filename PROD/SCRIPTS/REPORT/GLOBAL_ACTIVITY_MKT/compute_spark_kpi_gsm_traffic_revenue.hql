INSERT INTO AGG.SPARK_FT_GLOBAL_ACTIVITY_DAILY_MKT
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
    ,'MAIN' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(MAIN_RATED_AMOUNT) TOTAL_AMOUNT
    ,SUM(MAIN_RATED_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,NULL REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
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
    ,'PROMO' SUB_ACCOUNT
    ,'HIT' MEASUREMENT_UNIT
    , 'FT_GSM_TRAFFIC_REVENUE_DAILY' SOURCE_TABLE
    ,OPERATOR_CODE
    ,SUM(PROMO_RATED_AMOUNT) TOTAL_AMOUNT
    ,SUM(PROMO_RATED_AMOUNT) RATED_AMOUNT
    ,CURRENT_TIMESTAMP INSERT_DATE
    ,NULL REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
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
    ,NULL REGION_ID
    ,TRANSACTION_DATE
FROM AGG.SPARK_FT_GSM_TRAFFIC_REVENUE_DAILY
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