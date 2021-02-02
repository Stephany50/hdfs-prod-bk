INSERT INTO TMP.SPARK_TT_CLIENT_SITE_TRAFFIC_ON_90DAY
SELECT
         a.MSISDN ,
         max(a.SITE_NAME)SITE_NAME,
         max(a.TOWNNAME)TOWNNAME,
         max(a.ADMINISTRATIVE_REGION)ADMINISTRATIVE_REGION,
         max(a.COMMERCIAL_REGION)COMMERCIAL_REGION,
         max(cast(b.LAST_LOCATION_DAY as date)) LAST_LOCATION_DAY,
         max(a.OPERATOR_CODE)OPERATOR_CODE,
         current_timestamp  INSERT_DATE,
         '###SLICE_VALUE###' as E_DATE,
         max(a.LOCATION_CI)LOCATION_CI ,
         max(a.LOCATION_LAC)LOCATION_LAC
     FROM
     (
         SELECT
             MSISDN,
             m.SITE_NAME,
             m.TOWNNAME,
             nvl(m.LOCATION_CI,ci) LOCATION_CI,
             nvl(m.LOCATION_LAC,lac) LOCATION_LAC,
             m.ADMINISTRATIVE_REGION,
             m.COMMERCIAL_REGION,
             m.OPERATOR_CODE
         FROM
         (
             SELECT
                fn_format_msisdn_to_9digits(MSISDN) MSISDN,
                SITE_NAME,
                TOWNNAME,
                 location_ci,
                 LOCATION_LAC,
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
                LOCATION_CI,
                LOCATION_LAC,
                ADMINISTRATIVE_REGION,
                COMMERCIAL_REGION,
                OPERATOR_CODE
         ) m
         left join (select site_name , max(ci ) ci , max(lac) lac from DIM.spark_dt_gsm_cell_code group by site_name ) b on upper(trim(nvl(m.site_name,'ND'))) = upper(trim(nvl(b.site_name,'ND')))
         WHERE Rang = 1
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
    group by a.msisdn
