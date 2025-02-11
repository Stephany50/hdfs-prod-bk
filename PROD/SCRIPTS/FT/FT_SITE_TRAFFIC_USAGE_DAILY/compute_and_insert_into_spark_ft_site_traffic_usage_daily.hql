INSERT INTO MON.SPARK_FT_SITE_TRAFFIC_USAGE_DAILY
SELECT
    B.SITE_NAME
    , B.TOWNNAME
    , B.ADMINISTRATIVE_REGION
    , B.COMMERCIAL_REGION
    , OPERATOR_CODE
    , SUM(FAIL_IN) AS FAIL_IN
    , SUM(FAIL_OUT) AS FAIL_OUT
    , SUM(DUREE_SORTANT) AS DUREE_SORTANT
    , SUM(NBRE_TEL_SORTANT) AS NBRE_TEL_SORTANT
    , SUM(DUREE_ENTRANT) AS DUREE_ENTRANT
    , SUM(NBRE_TEL_ENTRANT) AS NBRE_TEL_ENTRANT
    , SUM(NBRE_SMS_SORTANT) AS NBRE_SMS_SORTANT
    , SUM(NBRE_SMS_ENTRANT) AS NBRE_SMS_ENTRANT
    , SUM(TOTAL_DUREE_SORTANT) AS TOTAL_DUREE_SORTANT
    , SUM(TOTAL_DUREE_ENTRANT) AS TOTAL_DUREE_ENTRANT
    , SUM(TOTAL_NBRE_SORTANT) AS TOTAL_NBRE_SORTANT
    , SUM(TOTAL_NBRE_ENTRANT) AS TOTAL_NBRE_ENTRANT
    ,'FT_CELL_TRAFIC_DAYLY' AS SOURCE_TABLE
    , PROFILE_NAME
    , CONTRACT_TYPE
    , B.TECHNOLOGIE
    , CURRENT_TIMESTAMP() AS INSERT_DATE
    , '###SLICE_VALUE###' EVENT_DATE
FROM
(
    SELECT *
    FROM MON.SPARK_FT_CELL_TRAFIC_DAYLY
    WHERE EVENT_DATE = '###SLICE_VALUE###'
) A
LEFT JOIN VW_SDT_CI_INFO_NEW B
ON SUBSTR(MS_LOCATION, 14, 5) = B.ci
GROUP BY B.SITE_NAME
    , B.TOWNNAME
    , B.ADMINISTRATIVE_REGION
    , B.COMMERCIAL_REGION
    , OPERATOR_CODE
    , PROFILE_NAME
    , CONTRACT_TYPE
    , B.TECHNOLOGIE