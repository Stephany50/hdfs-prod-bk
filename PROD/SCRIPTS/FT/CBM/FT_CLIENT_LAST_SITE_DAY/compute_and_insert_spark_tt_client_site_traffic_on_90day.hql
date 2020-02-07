INSERT INTO TMP.SPARK_TT_CLIENT_SITE_TRAFFIC_ON_90DAY
SELECT
         a.MSISDN ,
         a.SITE_NAME,
         a.TOWNNAME,
         a.ADMINISTRATIVE_REGION,
         a.COMMERCIAL_REGION,
         cast(b.LAST_LOCATION_DAY as date) LAST_LOCATION_DAY,
         a.OPERATOR_CODE,
         current_timestamp  INSERT_DATE,
         '###SLICE_VALUE###' as E_DATE
     FROM
     (
         SELECT
             MSISDN,
             SITE_NAME,
             TOWNNAME,
             ADMINISTRATIVE_REGION,
             COMMERCIAL_REGION,
             OPERATOR_CODE
         FROM
         (
             SELECT
                fn_format_msisdn_to_9digits(MSISDN) MSISDN,
                SITE_NAME,
                TOWNNAME,
                ADMINISTRATIVE_REGION,
                COMMERCIAL_REGION,
                OPERATOR_CODE,
                ROW_NUMBER() OVER (PARTITION BY fn_format_msisdn_to_9digits(MSISDN) ORDER BY SUM (NVL (DUREE_SORTANT, 0) + NVL (DUREE_ENTRANT, 0) + NVL (NBRE_SMS_SORTANT, 0)  + NVL (NBRE_SMS_ENTRANT, 0) ) DESC) AS Rang
             FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY a
             WHERE EVENT_DATE BETWEEN date_sub('###SLICE_VALUE###', 89) AND  '###SLICE_VALUE###'
             GROUP BY
                fn_format_msisdn_to_9digits(MSISDN),
                SITE_NAME,
                TOWNNAME,
                ADMINISTRATIVE_REGION,
                COMMERCIAL_REGION,
                OPERATOR_CODE
         ) m WHERE Rang = 1
     )a
     INNER JOIN
     (
         SELECT
         fn_format_msisdn_to_9digits(MSISDN) MSISDN,
         CAST(MAX(EVENT_DATE) AS VARCHAR(10) ) LAST_LOCATION_DAY
         FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY a
         WHERE EVENT_DATE BETWEEN date_sub('###SLICE_VALUE###', 89) AND  '###SLICE_VALUE###'
         GROUP BY fn_format_msisdn_to_9digits(MSISDN)
     ) b
     ON a.MSISDN = b.MSISDN