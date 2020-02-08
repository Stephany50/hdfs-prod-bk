INSERT INTO MON.SPARK_FT_CLIENT_LAST_SITE_DAY
SELECT
 MSISDN
 , TRIM(SITE_NAME) AS SITE_NAME
 , TOWNNAME
 , ADMINISTRATIVE_REGION
 , COMMERCIAL_REGION
 , LAST_LOCATION_DAY
 , OPERATOR_CODE
 , INSERT_DATE
 , EVENT_DATE
FROM (
    SELECT
        O.MSISDN MSISDN,
        nvl (N.SITE_NAME, O.SITE_NAME) SITE_NAME,
        nvl (N.TOWNNAME, O.TOWNNAME ) TOWNNAME,
        nvl (N.ADMINISTRATIVE_REGION, O.ADMINISTRATIVE_REGION ) ADMINISTRATIVE_REGION,
        nvl (N.COMMERCIAL_REGION, O.COMMERCIAL_REGION ) COMMERCIAL_REGION,
        nvl (N.LAST_LOCATION_DAY, O.LAST_LOCATION_DAY ) LAST_LOCATION_DAY,
        O.OPERATOR_CODE OPERATOR_CODE,
        N.INSERT_DATE INSERT_DATE,
        N.EVENT_DATE EVENT_DATE
    FROM TMP.SPARK_TT_CLIENT_LAST_SITE_DAY O
    LEFT JOIN TMP.SPARK_TT_CLIENT_SITE_TRAFFIC_ON_90DAY N ON O.MSISDN=N.MSISDN
    WHERE N.MSISDN IS NOT NULL
    UNION ALL
    SELECT
        N.MSISDN,
        N.SITE_NAME,
        N.TOWNNAME,
        N.ADMINISTRATIVE_REGION,
        N.COMMERCIAL_REGION,
        N.LAST_LOCATION_DAY,
        N.OPERATOR_CODE,
        N.INSERT_DATE,
        TO_DATE(N.EVENT_DATE) EVENT_DATE
    FROM TMP.SPARK_TT_CLIENT_LAST_SITE_DAY O
    LEFT JOIN TMP.SPARK_TT_CLIENT_SITE_TRAFFIC_ON_90DAY N ON O.MSISDN=N.MSISDN
    WHERE N.MSISDN IS  NULL
)T