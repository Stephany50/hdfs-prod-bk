INSERT INTO MON.SPARK_FT_CELL_360
SELECT
    CELL_NAME,
    CI,
    TECHNOLOGIE,
    DATA_USERS,
    VOICE_USERS,
    TOTAL_VOICE_REVENUE,
    TOTAL_VOICE_DURATION,
    TOTAL_SMS_REVENUE,
    ROAM_IN_VOICE_REVENUE,
    ROAM_OUT_VOICE_REVENUE,
    ROAM_IN_SMS_REVENUE,
    ROAM_OUT_SMS_REVENUE,
    ROAM_DATA_REVENUE,
    MAIN_RATED_TEL_AMOUNT,
    MAIN_RATED_TEL_OCM_AMOUNT,
    MAIN_RATED_TEL_MTN_AMOUNT,
    MAIN_RATED_TEL_NEXTTEL_AMOUNT,
    MAIN_RATED_TEL_CAMTEL_AMOUNT,
    MAIN_RATED_TEL_SET_AMOUNT,
    MAIN_RATED_TEL_ROAM_IN_AMOUNT,
    MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
    MAIN_RATED_TEL_SVA_AMOUNT,
    MAIN_RATED_TEL_INT_AMOUNT,
    MAIN_RATED_SMS_AMOUNT,
    DATA_MAIN_RATED_AMOUNT,
    DATA_GOS_MAIN_RATED_AMOUNT,
    DATA_ROAM_MAIN_RATED_AMOUNT,
    OG_RATED_CALL_DURATION,
    OG_TOTAL_CALL_DURATION,
    RATED_TEL_OCM_DURATION,
    RATED_TEL_MTN_DURATION,
    RATED_TEL_NEXTTEL_DURATION,
    RATED_TEL_CAMTEL_DURATION,
    RATED_TEL_SET_DURATION,
    RATED_TEL_ROAM_IN_DURATION,
    RATED_TEL_ROAM_OUT_DURATION,
    RATED_TEL_SVA_DURATION,
    RATED_TEL_INT_DURATION,
    OG_SMS_TOTAL_COUNT,
    OG_SMS_OCM_COUNT,
    OG_SMS_MTN_COUNT,
    OG_SMS_NEXTTEL_COUNT,
    OG_SMS_CAMTEL_COUNT,
    OG_SMS_SET_COUNT,
    OG_SMS_ROAM_IN_COUNT,
    OG_SMS_ROAM_OUT_COUNT,
    OG_SMS_SVA_COUNT,
    OG_SMS_INTERNATIONAL_COUNT,
    DATA_BYTES_RECEIVED,
    DATA_BYTES_SENT,
    DATA_BYTES_USED_IN_BUNDLE_ROAM,
    DATA_BYTES_USED_PAYGO_ROAM,
    DATA_BYTES_USED_OUT_BUNDLE_ROAM,
    DATA_BYTES_USED_IN_BUNDLE,
    DATA_PROMO_RATED_AMOUNT,
    DATA_ROAM_PROMO_RATED_AMOUNT,
    CURRENT_TIMESTAMP() INSERT_DATE,
    CATEGORY_SITE,
    lac,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT
        NVL(NVL(NVL(A.CI, B.CI), C.CI), D.CI) CI,
        NVL(NVL(NVL(A.lac, B.lac), C.lac), D.lac) lac,
        MAX(A.CELL_NAME) CELL_NAME,
        MAX(A.TECHNOLOGIE) TECHNOLOGIE,
        MAX(A.CATEGORY_SITE) CATEGORY_SITE,
        SUM(B.DATA_USERS) DATA_USERS,
        SUM(B.DATA_MAIN_RATED_AMOUNT) DATA_MAIN_RATED_AMOUNT,
        SUM(B.ROAM_DATA_REVENUE) ROAM_DATA_REVENUE,
        SUM(B.DATA_GOS_MAIN_RATED_AMOUNT) DATA_GOS_MAIN_RATED_AMOUNT,
        SUM(B.DATA_ROAM_MAIN_RATED_AMOUNT) DATA_ROAM_MAIN_RATED_AMOUNT,
        SUM(B.DATA_BYTES_SENT) DATA_BYTES_SENT,
        SUM(B.DATA_BYTES_RECEIVED) DATA_BYTES_RECEIVED,
        SUM(B.DATA_BYTES_USED_IN_BUNDLE_ROAM) DATA_BYTES_USED_IN_BUNDLE_ROAM,
        SUM(B.DATA_PROMO_RATED_AMOUNT) DATA_PROMO_RATED_AMOUNT,
        SUM(B.DATA_BYTES_USED_IN_BUNDLE) DATA_BYTES_USED_IN_BUNDLE,
        SUM(B.DATA_BYTES_USED_OUT_BUNDLE_ROAM) DATA_BYTES_USED_OUT_BUNDLE_ROAM,
        SUM(B.DATA_ROAM_PROMO_RATED_AMOUNT) DATA_ROAM_PROMO_RATED_AMOUNT,
        SUM(B.DATA_BYTES_USED_PAYGO_ROAM) DATA_BYTES_USED_PAYGO_ROAM,
        SUM(C.TOTAL_VOICE_REVENUE) TOTAL_VOICE_REVENUE,
        SUM(C.TOTAL_VOICE_DURATION) TOTAL_VOICE_DURATION,
        SUM(C.TOTAL_SMS_REVENUE) TOTAL_SMS_REVENUE,
        SUM(C.ROAM_IN_VOICE_REVENUE) ROAM_IN_VOICE_REVENUE,
        SUM(C.ROAM_OUT_VOICE_REVENUE) ROAM_OUT_VOICE_REVENUE,
        SUM(C.ROAM_IN_SMS_REVENUE) ROAM_IN_SMS_REVENUE,
        SUM(C.ROAM_OUT_SMS_REVENUE) ROAM_OUT_SMS_REVENUE,
        SUM(C.MAIN_RATED_TEL_AMOUNT) MAIN_RATED_TEL_AMOUNT,
        SUM(C.MAIN_RATED_TEL_ROAM_IN_AMOUNT) MAIN_RATED_TEL_ROAM_IN_AMOUNT,
        SUM(C.MAIN_RATED_TEL_ROAM_OUT_AMOUNT) MAIN_RATED_TEL_ROAM_OUT_AMOUNT,
        SUM(C.MAIN_RATED_TEL_SVA_AMOUNT) MAIN_RATED_TEL_SVA_AMOUNT,
        SUM(C.MAIN_RATED_TEL_INT_AMOUNT) MAIN_RATED_TEL_INT_AMOUNT,
        SUM(C.MAIN_RATED_SMS_AMOUNT) MAIN_RATED_SMS_AMOUNT,
        SUM(C.OG_RATED_CALL_DURATION) OG_RATED_CALL_DURATION,
        SUM(C.OG_TOTAL_CALL_DURATION) OG_TOTAL_CALL_DURATION,
        SUM(C.RATED_TEL_OCM_DURATION) RATED_TEL_OCM_DURATION,
        SUM(C.RATED_TEL_MTN_DURATION) RATED_TEL_MTN_DURATION,
        SUM(C.RATED_TEL_NEXTTEL_DURATION) RATED_TEL_NEXTTEL_DURATION,
        SUM(C.RATED_TEL_CAMTEL_DURATION) RATED_TEL_CAMTEL_DURATION,
        SUM(C.RATED_TEL_SET_DURATION) RATED_TEL_SET_DURATION,
        SUM(C.RATED_TEL_ROAM_IN_DURATION) RATED_TEL_ROAM_IN_DURATION,
        SUM(C.RATED_TEL_ROAM_OUT_DURATION) RATED_TEL_ROAM_OUT_DURATION,
        SUM(C.RATED_TEL_SVA_DURATION) RATED_TEL_SVA_DURATION,
        SUM(C.RATED_TEL_INT_DURATION) RATED_TEL_INT_DURATION,
        SUM(C.OG_SMS_TOTAL_COUNT) OG_SMS_TOTAL_COUNT,
        SUM(C.OG_SMS_OCM_COUNT) OG_SMS_OCM_COUNT,
        SUM(C.OG_SMS_MTN_COUNT) OG_SMS_MTN_COUNT,
        SUM(C.OG_SMS_NEXTTEL_COUNT) OG_SMS_NEXTTEL_COUNT,
        SUM(C.OG_SMS_CAMTEL_COUNT) OG_SMS_CAMTEL_COUNT,
        SUM(C.OG_SMS_SET_COUNT) OG_SMS_SET_COUNT,
        SUM(C.OG_SMS_ROAM_IN_COUNT) OG_SMS_ROAM_IN_COUNT,
        SUM(C.OG_SMS_ROAM_OUT_COUNT) OG_SMS_ROAM_OUT_COUNT,
        SUM(C.OG_SMS_SVA_COUNT) OG_SMS_SVA_COUNT,
        SUM(C.OG_SMS_INTERNATIONAL_COUNT) OG_SMS_INTERNATIONAL_COUNT,
        SUM(C.MAIN_RATED_TEL_MTN_AMOUNT) MAIN_RATED_TEL_MTN_AMOUNT,
        SUM(C.MAIN_RATED_TEL_CAMTEL_AMOUNT) MAIN_RATED_TEL_CAMTEL_AMOUNT,
        SUM(C.MAIN_RATED_TEL_NEXTTEL_AMOUNT) MAIN_RATED_TEL_NEXTTEL_AMOUNT,
        SUM(C.MAIN_RATED_TEL_OCM_AMOUNT) MAIN_RATED_TEL_OCM_AMOUNT,
        SUM(C.MAIN_RATED_TEL_SET_AMOUNT) MAIN_RATED_TEL_SET_AMOUNT,
        SUM(D.VOICE_USERS) VOICE_USERS
    FROM
    (
        SELECT
            NVL(A0.CI, A1.CI) CI,
            nvl(A0.lac, A1.lac) lac,
            NVL(A0.CELL_NAME, A1.CELL_NAME) CELL_NAME,
            A0.TECHNOLOGIE TECHNOLOGIE,
            NVL(A0.CATEGORY_SITE, A1.CATEGORY_SITE) CATEGORY_SITE
        FROM
        (
            SELECT
                cast(ci as int) CI
                , cast(lac as int) lac
                , MAX(TECHNOLOGIE) TECHNOLOGIE
                , MAX(CELLNAME) CELL_NAME
                , MAX(CATEGORIE_SITE) CATEGORY_SITE
            FROM DIM.SPARK_DT_GSM_CELL_CODE
            GROUP BY cast(ci as int), cast(lac as int)
        ) A0
        FULL JOIN
        (
            SELECT
                cast(ci as int) CI,
                cast(lac as int) lac,
                MAX(CELLNAME) CELL_NAME,
                'AMN' CATEGORY_SITE
            FROM DIM.DT_CI_LAC_SITE_AMN
            GROUP BY cast(ci as int), cast(lac as int)
        ) A1
        ON A0.CI = A1.CI and A0.lac = A1.lac
    ) A
    FULL JOIN
    (
        SELECT
            CAST(LOCATION_CI AS INT) CI
            , CAST(location_lac AS INT) LAC
            , NVL(COUNT(DISTINCT (CASE WHEN BYTES_SENT + BYTES_RECEIVED >= 1048576 THEN CHARGED_PARTY_MSISDN END)), 0) DATA_USERS
            , NVL(SUM(CASE WHEN NVL(SDP_GOS_SERV_NAME, 'NOT_GOS') = 'NOT_GOS' THEN NVL(MAIN_COST, 0) ELSE 0 END), 0) DATA_MAIN_RATED_AMOUNT
            , NVL(SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(MAIN_COST, 0) ELSE 0 END), 0) ROAM_DATA_REVENUE
            , NVL(SUM(CASE WHEN UPPER(SERVICE_CODE) LIKE '%GOS%SDP%' THEN NVL(MAIN_COST, 0) ELSE 0 END), 0) DATA_GOS_MAIN_RATED_AMOUNT
            , SUM(NVL(PROMO_COST, 0)) DATA_PROMO_RATED_AMOUNT
            , SUM(NVL(BUNDLE_BYTES_USED_VOLUME, 0)) DATA_BYTES_USED_IN_BUNDLE
            , NVL(SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(MAIN_COST, 0) ELSE 0 END), 0) DATA_ROAM_MAIN_RATED_AMOUNT 
            , NVL(SUM(NVL(BYTES_SENT, 0)), 0) DATA_BYTES_SENT
            , NVL(SUM(NVL(BYTES_RECEIVED, 0)), 0) DATA_BYTES_RECEIVED
            , NVL(SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END), 0) DATA_BYTES_USED_IN_BUNDLE_ROAM
            , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(BYTES_SENT, 0) + NVL(BYTES_RECEIVED, 0) - NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END) DATA_BYTES_USED_OUT_BUNDLE_ROAM
            , SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(PROMO_COST, 0) ELSE 0 END) DATA_ROAM_PROMO_RATED_AMOUNT
            , NVL(SUM(CASE WHEN NVL(ROAMING_INDICATOR, 0) = 1 THEN NVL(BYTES_SENT, 0) + NVL(BYTES_RECEIVED, 0) - NVL(BUNDLE_BYTES_USED_VOLUME, 0) ELSE 0 END), 0) DATA_BYTES_USED_PAYGO_ROAM
        FROM MON.SPARK_FT_CRA_GPRS
        WHERE SESSION_DATE = '###SLICE_VALUE###' AND NVL(MAIN_COST, 0) >= 0
        GROUP BY CAST(LOCATION_CI AS INT), CAST(location_lac AS INT)
    ) B ON A.CI = B.CI and A.lac = B.lac
    FULL JOIN
    (
        SELECT
            C0.LOCATION_CI  CI
            , C0.location_lac lac
            ,NVL(SUM (CASE WHEN C0.SERVICE_CODE = 'VOI_VOX' THEN  (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT)   ELSE  0    END ), 0)  AS TOTAL_VOICE_REVENUE
            ,NVL(SUM (CASE WHEN (MAIN_RATED_AMOUNT) > 0 THEN  DURATION   ELSE  0    END), 0)  TOTAL_VOICE_DURATION
            ,NVL(SUM (CASE WHEN C0.SERVICE_CODE = 'NVX_SMS' THEN  (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT)   ELSE  0    END ) , 0)  TOTAL_SMS_REVENUE
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS', 'VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN (MAIN_RATED_AMOUNT + PROMO_RATED_AMOUNT) ELSE 0 END ), 0)  ROAM_IN_VOICE_REVENUE
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ), 0)   ROAM_OUT_VOICE_REVENUE
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN MAIN_RATED_AMOUNT ELSE 0 END ), 0)   ROAM_IN_SMS_REVENUE
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN MAIN_RATED_AMOUNT ELSE 0 END ), 0)   ROAM_OUT_SMS_REVENUE
            ,NVL(SUM (CASE WHEN C0.SERVICE_CODE = 'VOI_VOX' THEN  MAIN_RATED_AMOUNT  ELSE  0    END ), 0) MAIN_RATED_TEL_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ), 0)  MAIN_RATED_TEL_ROAM_IN_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ), 0)   MAIN_RATED_TEL_ROAM_OUT_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') THEN MAIN_RATED_AMOUNT ELSE 0 END ), 0)   MAIN_RATED_TEL_SVA_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) , 0)  MAIN_RATED_TEL_INT_AMOUNT
            ,NVL((SUM (MAIN_RATED_AMOUNT) - (SUM (CASE WHEN  C0.SERVICE_CODE = 'VOI_VOX' THEN  MAIN_RATED_AMOUNT  ELSE  0    END ) + SUM (CASE WHEN SERVICE_CODE NOT IN ('NVX_SMS','VOI_VOX') THEN MAIN_RATED_AMOUNT ELSE 0 END ) )), 0)   MAIN_RATED_SMS_AMOUNT 
            ,NVL(SUM (CASE WHEN  (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT) > 0 THEN  DURATION   ELSE  0    END), 0)  OG_RATED_CALL_DURATION
            ,NVL(SUM (DURATION), 0) OG_TOTAL_CALL_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN DURATION ELSE 0 END ) , 0) RATED_TEL_OCM_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN RATED_DURATION ELSE 0 END ) , 0) RATED_TEL_MTN_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN RATED_DURATION ELSE 0 END ) , 0) RATED_TEL_NEXTTEL_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN RATED_DURATION ELSE 0 END ) , 0) RATED_TEL_CAMTEL_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN RATED_DURATION ELSE 0 END ) , 0) RATED_TEL_SET_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN RATED_DURATION ELSE 0 END ) , 0) RATED_TEL_ROAM_IN_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN RATED_DURATION ELSE 0 END ) , 0) RATED_TEL_ROAM_OUT_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA') AND (PROMO_RATED_AMOUNT + MAIN_RATED_AMOUNT) > 0 THEN DURATION ELSE 0 END ) , 0) RATED_TEL_SVA_DURATION
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO','OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') THEN RATED_DURATION ELSE 0 END ), 0) RATED_TEL_INT_DURATION
            ,NVL(SUM (CASE WHEN C0.SERVICE_CODE = 'NVX_SMS' THEN  1  ELSE  0    END ) , 0) OG_SMS_TOTAL_COUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN 1 ELSE 0 END ) , 0)  OG_SMS_OCM_COUNT
            ,NVL(SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') AND SERVICE_CODE = 'NVX_SMS' THEN 1 ELSE 0 END ) , 0) OG_SMS_MTN_COUNT
            ,NVL(SUM (CASE WHEN DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN 1 ELSE 0 END ) , 0)  OG_SMS_NEXTTEL_COUNT
            ,NVL(SUM (CASE WHEN DEST_OPERATOR = 'OUT_NAT_MOB_CAM' AND SERVICE_CODE = 'NVX_SMS' THEN 1 ELSE 0 END ) , 0) OG_SMS_CAMTEL_COUNT 
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN 1 ELSE 0 END ) , 0) OG_SMS_SET_COUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('IN_ROAM_MT') THEN 1 ELSE 0 END ) , 0)  OG_SMS_ROAM_IN_COUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_ROAM_MO') THEN 1 ELSE 0 END ) , 0) OG_SMS_ROAM_OUT_COUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('NVX_SMS') AND DEST_OPERATOR IN ('OUT_SVA','OUT_CCSVA')  THEN 1 ELSE 0 END ) , 0)  OG_SMS_SVA_COUNT
            ,NVL(SUM (CASE WHEN DEST_OPERATOR NOT IN ('OUT_NAT_MOB_OCM','OUT_NAT_MOB_MTN','OUT_NAT_MOB_CAM','OUT_SVA','OUT_CCSVA','OUT_NAT_MOB_MVO', 'OUT_ROAM_MO','IN_ROAM_MT','OUT_NAT_MOB_NEX') AND SERVICE_CODE = 'NVX_SMS' THEN 1 ELSE 0 END ) , 0) OG_SMS_INTERNATIONAL_COUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MTN') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END )  , 0) MAIN_RATED_TEL_MTN_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_CAM') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) , 0) MAIN_RATED_TEL_CAMTEL_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_NEX') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ) , 0) MAIN_RATED_TEL_NEXTTEL_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_OCM') THEN MAIN_RATED_AMOUNT ELSE 0 END ) , 0) MAIN_RATED_TEL_OCM_AMOUNT
            ,NVL(SUM (CASE WHEN SERVICE_CODE IN ('VOI_VOX') AND DEST_OPERATOR IN ('OUT_NAT_MOB_MVO') THEN NVL(MAIN_RATED_AMOUNT,0) ELSE 0 END ),  0) MAIN_RATED_TEL_SET_AMOUNT
        FROM
        (
            SELECT
                CAST(CONV(C00.LOCATION_CI, 16, 10) AS INT) LOCATION_CI
                , CAST(CONV(location_lac, 16, 10) AS INT) location_lac
                , (
                    CASE
                    WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                    WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                    WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                    WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                    WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                    WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                    WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                    WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                    WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                    WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                    WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                    WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                    ELSE 'AUT'
                    END
                ) SERVICE_CODE
                , (
                    CASE WHEN C00.Call_Destination_Code IN ('ONNET','ONNETFREE','OCM_D') THEN 'OUT_NAT_MOB_OCM'
                    WHEN C00.Call_Destination_Code IN ('MTN','MTN_D') THEN 'OUT_NAT_MOB_MTN'
                    WHEN C00.Call_Destination_Code IN ('CAM_D','CAM') THEN 'OUT_NAT_MOB_CAM'
                    WHEN C00.Call_Destination_Code IN ('NEXTTEL','NEXTTEL_D') THEN 'OUT_NAT_MOB_NEX'
                    WHEN C00.Call_Destination_Code = 'VAS' THEN 'OUT_SVA'
                    WHEN C00.Call_Destination_Code = 'EMERG' THEN 'OUT_CCSVA'
                    WHEN C00.Call_Destination_Code = 'OCRMG' THEN 'OUT_ROAM_MO'
                    WHEN C00.Call_Destination_Code = 'TCRMG' THEN 'IN_ROAM_MT'
                    WHEN C00.Call_Destination_Code = 'INT' THEN 'OUT_INT'
                    WHEN C00.Call_Destination_Code = 'MVNO' THEN 'OUT_NAT_MOB_MVO'
                    ELSE Call_Destination_Code END
                ) DEST_OPERATOR
                , PROMO_RATED_AMOUNT
                , MAIN_RATED_AMOUNT
                , CALL_PROCESS_TOTAL_DURATION DURATION
                , CASE WHEN C00.Main_Rated_Amount + C00.Promo_Rated_Amount > 0 THEN CALL_PROCESS_TOTAL_DURATION ELSE 0 END AS RATED_DURATION
            FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID C00
            WHERE TRANSACTION_DATE ='###SLICE_VALUE###'
                AND Main_Rated_Amount >= 0
                AND Promo_Rated_Amount >= 0
        ) C0
        GROUP BY C0.LOCATION_CI, C0.location_lac
    ) C ON A.CI = C.CI and a.lac = c.lac
    FULL JOIN
    (
        SELECT
            NVL(COUNT(DISTINCT CHARGED_PARTY), 0) VOICE_USERS
            , CAST(CONV(LOCATION_CI, 16, 10) AS INT) CI
            , CAST(CONV(location_lac, 16, 10) AS INT) lac
        FROM MON.SPARK_FT_BILLED_TRANSACTION_PREPAID
        WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
            AND MAIN_RATED_AMOUNT >= 0
            AND PROMO_RATED_AMOUNT >= 0
            AND CASE
                    WHEN SERVICE_CODE = 'SMS' THEN 'NVX_SMS'
                    WHEN SERVICE_CODE = 'TEL' THEN 'VOI_VOX'
                    WHEN SERVICE_CODE = 'USS' THEN 'NVX_USS'
                    WHEN SERVICE_CODE = 'GPR' THEN 'NVX_DAT_GPR'
                    WHEN SERVICE_CODE = 'DFX' THEN 'NVX_DFX'
                    WHEN SERVICE_CODE = 'DAT' THEN 'NVX_DAT'
                    WHEN SERVICE_CODE = 'VDT' THEN 'NVX_VDT'
                    WHEN SERVICE_CODE = 'WEB' THEN 'NVX_WEB'
                    WHEN UPPER(SERVICE_CODE) IN ('SMSMO','SMSRMG') THEN 'NVX_SMS'
                    WHEN UPPER(SERVICE_CODE) IN ('OC','OCFWD','OCRMG','TCRMG') THEN 'VOI_VOX'
                    WHEN UPPER(SERVICE_CODE) LIKE '%FNF%MODIFICATION%' THEN 'VOI_VOX'
                    WHEN UPPER(SERVICE_CODE) LIKE '%ACCOUNT%INTERRO%' THEN 'VOI_VOX'
                    ELSE 'AUT'
                END = 'VOI_VOX'
        GROUP BY CAST(CONV(LOCATION_CI, 16, 10) AS INT), CAST(CONV(location_lac, 16, 10) AS INT)
    ) D ON A.CI = D.CI and a.lac = d.lac
    GROUP BY NVL(NVL(NVL(A.CI, B.CI), C.CI), D.CI), NVL(NVL(NVL(A.lac, B.lac), C.lac), D.lac)
) T