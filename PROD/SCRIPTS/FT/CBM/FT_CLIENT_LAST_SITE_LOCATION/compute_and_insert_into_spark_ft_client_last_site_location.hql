INSERT INTO MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION
SELECT
    NVL(B.MSISDN, A.MSISDN) MSISDN
    , IF(A.MSISDN IS NULL OR B.MSISDN IS NULL, B.SITE_NAME, NVL(B.SITE_NAME, A.SITE_NAME)) SITE_NAME
    , IF(A.MSISDN IS NULL OR B.MSISDN IS NULL, B.LAST_LOCATION_MONTH, NVL(B.LAST_LOCATION_MONTH, A.LAST_LOCATION_MONTH)) LAST_LOCATION_MONTH
    , IF(A.MSISDN IS NULL OR B.MSISDN IS NULL, B.TOWNNAME, NVL(B.TOWNNAME, A.TOWNNAME)) TOWNNAME
    , IF(A.MSISDN IS NULL OR B.MSISDN IS NULL, B.ADMINISTRATIVE_REGION, NVL(B.ADMINISTRATIVE_REGION, A.ADMINISTRATIVE_REGION)) ADMINISTRATIVE_REGION
    , IF(A.MSISDN IS NULL OR B.MSISDN IS NULL, B.COMMERCIAL_REGION, NVL(B.COMMERCIAL_REGION, A.COMMERCIAL_REGION)) COMMERCIAL_REGION
    , IF(A.MSISDN IS NULL OR B.MSISDN IS NULL, B.OPERATOR_CODE, NVL(B.OPERATOR_CODE, A.OPERATOR_CODE)) OPERATOR_CODE
    , IF(A.MSISDN IS NULL OR B.MSISDN IS NULL, B.REFRESH_DATE, NVL(B.REFRESH_DATE, A.REFRESH_DATE)) REFRESH_DATE
    , CURRENT_TIMESTAMP() INSERT_DATE
    , '###SLICE_VALUE###' EVENT_MONTH
FROM
(
    SELECT *
    FROM MON.SPARK_FT_CLIENT_LAST_SITE_LOCATION
    WHERE EVENT_MONTH = SUBSTRING(ADD_MONTHS('###SLICE_VALUE###' || '-01', -1), 1, 7)
        AND (
            LENGTH(FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN)) = 9
            OR (LENGTH (FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN)) <> 9 AND LAST_LOCATION_MONTH > SUBSTRING(ADD_MONTHS('###SLICE_VALUE###' || '-01', -6), 1, 7))
        )
        AND CAST(SITE_NAME AS DOUBLE) IS NULL
) A
FULL JOIN
(
    SELECT
        MAX (EVENT_MONTH) EVENT_MONTH
        , FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN) MSISDN
        , MAX (SITE_NAME) SITE_NAME
        , MAX (EVENT_MONTH) LAST_LOCATION_MONTH
        , TOWNNAME
        , ADMINISTRATIVE_REGION
        , COMMERCIAL_REGION
        , OPERATOR_CODE
        , CURRENT_DATE() REFRESH_DATE
    FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_MONTH
    WHERE EVENT_MONTH = '###SLICE_VALUE###' AND CAST(SITE_NAME AS DOUBLE) IS NULL
    GROUP BY FN_FORMAT_MSISDN_TO_9DIGITS(MSISDN)
        , SITE_NAME
        , TOWNNAME
        , ADMINISTRATIVE_REGION
        , COMMERCIAL_REGION
        , OPERATOR_CODE
) B
ON A.MSISDN = B.MSISDN
