INSERT INTO TMP.TT_A_SUBSCRIBER_SUMMARY_B2B
SELECT
    IF(
        A.FAMILLE_KPI IS NULL
        OR B.FAMILLE_KPI IS NULL
        OR A.DATECODE IS NULL
        OR B.DATECODE IS NULL
        OR A.SUBSCRIBER_TYPE IS NULL
        OR B.SUBSCRIBER_TYPE IS NULL
        OR A.OFFER_NAME IS NULL
        OR B.OFFER_NAME IS NULL
        OR A.COMMERCIAL_OFFER IS NULL
        OR B.COMMERCIAL_OFFER IS NULL
        , B.FAMILLE_KPI
        , A.FAMILLE_KPI
    ) FAMILLE_KPI
    , IF(
        A.FAMILLE_KPI IS NULL
        OR B.FAMILLE_KPI IS NULL
        OR A.DATECODE IS NULL
        OR B.DATECODE IS NULL
        OR A.SUBSCRIBER_TYPE IS NULL
        OR B.SUBSCRIBER_TYPE IS NULL
        OR A.OFFER_NAME IS NULL
        OR B.OFFER_NAME IS NULL
        OR A.COMMERCIAL_OFFER IS NULL
        OR B.COMMERCIAL_OFFER IS NULL
        , B.DATECODE
        , A.DATECODE
    ) DATECODE
    , IF(
        A.FAMILLE_KPI IS NULL
        OR B.FAMILLE_KPI IS NULL
        OR A.DATECODE IS NULL
        OR B.DATECODE IS NULL
        OR A.SUBSCRIBER_TYPE IS NULL
        OR B.SUBSCRIBER_TYPE IS NULL
        OR A.OFFER_NAME IS NULL
        OR B.OFFER_NAME IS NULL
        OR A.COMMERCIAL_OFFER IS NULL
        OR B.COMMERCIAL_OFFER IS NULL
        , B.SUBSCRIBER_TYPE
        , A.SUBSCRIBER_TYPE
    ) SUBSCRIBER_TYPE
    , IF(
        A.FAMILLE_KPI IS NULL
        OR B.FAMILLE_KPI IS NULL
        OR A.DATECODE IS NULL
        OR B.DATECODE IS NULL
        OR A.SUBSCRIBER_TYPE IS NULL
        OR B.SUBSCRIBER_TYPE IS NULL
        OR A.OFFER_NAME IS NULL
        OR B.OFFER_NAME IS NULL
        OR A.COMMERCIAL_OFFER IS NULL
        OR B.COMMERCIAL_OFFER IS NULL
        , B.OFFER_NAME
        , A.OFFER_NAME
    ) OFFER_NAME
    , IF(
        A.FAMILLE_KPI IS NULL
        OR B.FAMILLE_KPI IS NULL
        OR A.DATECODE IS NULL
        OR B.DATECODE IS NULL
        OR A.SUBSCRIBER_TYPE IS NULL
        OR B.SUBSCRIBER_TYPE IS NULL
        OR A.OFFER_NAME IS NULL
        OR B.OFFER_NAME IS NULL
        OR A.COMMERCIAL_OFFER IS NULL
        OR B.COMMERCIAL_OFFER IS NULL
        , B.COMMERCIAL_OFFER
        , A.COMMERCIAL_OFFER
    ) COMMERCIAL_OFFER
    , IF(
        A.FAMILLE_KPI IS NULL
        OR B.FAMILLE_KPI IS NULL
        OR A.DATECODE IS NULL
        OR B.DATECODE IS NULL
        OR A.SUBSCRIBER_TYPE IS NULL
        OR B.SUBSCRIBER_TYPE IS NULL
        OR A.OFFER_NAME IS NULL
        OR B.OFFER_NAME IS NULL
        OR A.COMMERCIAL_OFFER IS NULL
        OR B.COMMERCIAL_OFFER IS NULL
        , 0
        , B.TOTAL_COUNT
    ) TOTAL_COUNT
FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY_B2B A
FULL JOIN
(
    SELECT
        'Activation' FAMILLE_KPI
        , DATECODE
        , SUBSCRIBER_TYPE
        , OFFER_NAME
        , COMMERCIAL_OFFER
        , SUM(TOTAL_ACTIVATION) TOTAL_COUNT
    FROM
    (
        SELECT *
        FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY
        WHERE DATECODE >= '2017-01-01'
            AND NETWORK_DOMAIN = 'GSM'
    ) B0
    LEFT JOIN DIM.DT_OFFER_PROFILES B1
    ON UPPER(COMMERCIAL_OFFER) = PROFILE_CODE
    WHERE NVL(SEGMENTATION, 'B2C') = 'B2B'
    GROUP BY DATECODE, SUBSCRIBER_TYPE, OFFER_NAME, COMMERCIAL_OFFER

    UNION

    SELECT
        'Churn' FAMILLE_KPI
        , DATECODE
        , SUBSCRIBER_TYPE
        , OFFER_NAME
        , COMMERCIAL_OFFER
        , SUM(TOTAL_DEACTIVATION) TOTAL_COUNT
    FROM
    (
        SELECT *
        FROM AGG.SPARK_FT_A_SUBSCRIBER_SUMMARY
        WHERE DATECODE >= '2017-01-01'
            AND NETWORK_DOMAIN = 'GSM'
    ) B2
    LEFT JOIN DIM.DT_OFFER_PROFILES B3
    ON UPPER(COMMERCIAL_OFFER) = PROFILE_CODE
    WHERE NVL(SEGMENTATION, 'B2C') = 'B2B'
    GROUP BY DATECODE, SUBSCRIBER_TYPE, OFFER_NAME, COMMERCIAL_OFFER
)  B
ON (A.FAMILLE_KPI = B.FAMILLE_KPI AND A.DATECODE = B.DATECODE AND A.SUBSCRIBER_TYPE = B.SUBSCRIBER_TYPE AND A.OFFER_NAME = B.OFFER_NAME AND A.COMMERCIAL_OFFER = B.COMMERCIAL_OFFER)
