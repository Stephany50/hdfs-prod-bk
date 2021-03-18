INSERT INTO TMP.SPARK_TMP_SITE_360
SELECT
    nvl(F10.MSISDN, F11.MSISDN) MSISDN,
    UPPER(NVL(F11.SITE_NAME, F10.SITE_NAME)) SITE_NAME
FROM
(
    SELECT
        MSISDN,
        MAX(SITE_NAME) SITE_NAME
    FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY
    WHERE EVENT_DATE = '###SLICE_VALUE###'
    GROUP BY MSISDN
) F10
FULL JOIN
(
    SELECT
        MSISDN,
        MAX(SITE_NAME) SITE_NAME
    FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
    WHERE EVENT_DATE = '###SLICE_VALUE###'
    GROUP BY MSISDN
) F11
ON F10.MSISDN = F11.MSISDN
