INSERT INTO MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR
SELECT
    a.MSISDN
    , hour_period
    , IDENTIFICATEUR
    , CONTRACT_TYPE
    , COMMERCIAL_OFFER
    , ACTIVATION_DATE
    , DEACTIVATION_DATE
    , (
        CASE
            WHEN DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')>=-90 or
                DATEDIFF(OG_CALL, '###SLICE_VALUE###')>=-90 THEN 'OUI'
            ELSE 'NON'
        END
    ) EST_PARC_ART
    , (
        CASE
            WHEN (DATEDIFF(IC_CALL_1, '###SLICE_VALUE###')>=-90 and
                DATEDIFF(IC_CALL_2, '###SLICE_VALUE###')>=-90 and
                DATEDIFF(IC_CALL_3, '###SLICE_VALUE###')>=-90 and
                DATEDIFF(IC_CALL_4, '###SLICE_VALUE###')>=-90) OR
                DATEDIFF(OG_CALL, '###SLICE_VALUE###')>=-90 THEN 'OUI'
            ELSE 'NON'
        END
    ) EST_PARC_GROUPE
    , LAST_TRANSACTION_DATE_TIME
    , LAST_TRANSACTION_TYPE
    , SITE_NAME
    , TOWN
    , REGION
    , COMMERCIAL_REGION
    , current_timestamp insert_date
    , '###SLICE_VALUE###' event_date
FROM
(
    SELECT 
        MSISDN,
        HOUR_PERIOD,
        LAST_TRANSACTION_DATE_TIME,
        LAST_TRANSACTION_TYPE,
        CAST(LAST_CI AS BIGINT) LAST_CI,
        CAST(LAST_LAC AS BIGINT) LAST_LAC
    FROM MON.SPARK_FT_CLIENT_CELL_TRAFFIC_HOUR
    WHERE EVENT_DATE = '###SLICE_VALUE###'
) A
LEFT JOIN
(
    SELECT
        ACCESS_KEY MSISDN
        , OSP_CONTRACT_TYPE CONTRACT_TYPE
        , COMMERCIAL_OFFER
        , ACTIVATION_DATE
        , DEACTIVATION_DATE
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE EVENT_DATE = DATE_ADD('###SLICE_VALUE###',1)
        AND OSP_STATUS IN ('ACTIVE', 'INACTIVE')
) B on a.msisdn = b.msisdn
LEFT JOIN
(   
    SELECT
        MSISDN
       , MAX(IDENTIFICATEUR) IDENTIFICATEUR
    FROM DIM.SPARK_DT_BASE_IDENTIFICATION
    GROUP BY MSISDN
) E on a.msisdn = e.msisdn
LEFT JOIN
(
    SELECT
        MSISDN
        , MAX(OG_CALL) OG_CALL
        , MAX(IC_CALL_1) IC_CALL_1
        , MAX(IC_CALL_2) IC_CALL_2
        , MAX(IC_CALL_3) IC_CALL_3
        , MAX(IC_CALL_4) IC_CALL_4
    FROM MON.SPARK_FT_OG_IC_CALL_SNAPSHOT
    WHERE EVENT_DATE=DATE_ADD('###SLICE_VALUE###',1)
    GROUP BY MSISDN
) C on a.msisdn = c.msisdn
LEFT JOIN
(
    SELECT
        CAST(CI AS BIGINT) CI
        , CAST(LAC AS BIGINT) LAC
        , MAX(UPPER(SITE_NAME)) SITE_NAME
        , MAX(UPPER(TOWNNAME)) TOWN
        , MAX(UPPER(REGION)) REGION
        , MAX(UPPER(COMMERCIAL_REGION)) COMMERCIAL_REGION
    FROM DIM.SPARK_DT_GSM_CELL_CODE_NEW
    GROUP BY CI, LAC
) D ON A.LAST_CI = D.CI AND A.LAST_LAC = D.LAC

