select
    MSISDN,
    TER_TAC_CODE,
    TER_IMEI,
    TER_CONSTRUCTOR,
    TER_MODEL,
    TER_2G_3G_4G_COMPATIBILITY,
    TER_2G_COMPATIBILITY,
    TER_3G_COMPATIBILITY,
    TER_4G_COMPATIBILITY
from (
    SELECT
        MSISDN,
        SUBSTR(IMEI_MOST_USED,1,8) AS TER_TAC_CODE,
        SUBSTR(IMEI_MOST_USED,1,8) AS TER_IMEI,
        CONSTRUCTOR AS TER_CONSTRUCTOR,
        MODEL AS TER_MODEL,
        TECHNOLOGIE AS TER_2G_3G_4G_COMPATIBILITY,
        (CASE WHEN TECHNOLOGIE = '2G' THEN 'O' ELSE 'N' END) AS TER_2G_COMPATIBILITY,
        (CASE WHEN TECHNOLOGIE IN ('2.5G', '2.75G', '3G') THEN 'O' ELSE 'N' END) AS TER_3G_COMPATIBILITY,
        (CASE WHEN TECHNOLOGIE = '4G' THEN 'O' ELSE 'N' END) AS TER_4G_COMPATIBILITY
    FROM
    (
      SELECT
        DISTINCT
         MSISDN,
          IMEI_MOST_USED
      FROM
      (
         SELECT
            MSISDN
             , FIRST_VALUE(imei) OVER (PARTITION BY MSISDN ORDER BY TOTAL_DAYS_COUNT DESC) IMEI_MOST_USED
         FROM (
            select
                msisdn,
                substr(imei, 1, 14) imei,
                count(DISTINCT sdate) total_days_count
            from MON.FT_IMEI_ONLINE
            where sdate='2019-09-22'
            GROUP BY substr(imei, 1, 14), msisdn
         )a
         --WHERE SMONTH = TO_CHAR(ADD_MONTHS(TO_DATE(s_slice_value, 'yyyymmdd'), -1), 'yyyymm')
      )a
    )a
    LEFT JOIN  DIM.DT_HANDSET_REF ON SUBSTR(IMEI_MOST_USED,1,8) = TAC_CODE
)d