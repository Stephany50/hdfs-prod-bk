SELECT 
    b0.SERVED_MSISDN,
    b0.SERVED_IMSI,b0.OTHER_PARTY,
    b96.REGION,b96.TOWNNAME,b96.SITE_NAME,
    b0.EVENT_DATE
FROM (
   SELECT 
    SERVED_MSISDN,
    SERVED_IMSI,
    OTHER_PARTY,
    '###SLICE_VALUE###' EVENT_DATE
   FROM
      MON.SPARK_FT_MSC_TRANSACTION
   WHERE TRANSACTION_DATE = TO_DATE('###SLICE_VALUE###') AND OTHER_PARTY IN ('900','950', '951', '9111', '955', '8950', '8951', '89111', '8955')
   AND SERVED_MSISDN NOT IN (
      SELECT SERVED_MSISDN FROM TMP.TT_CALL_NEW_CUSTOMER_SERVICE
   ) 
) b0
LEFT JOIN
(
    SELECT
        nvl(b50.MSISDN, b51.MSISDN) MSISDN,
        UPPER(NVL(b51.SITE_NAME, b50.SITE_NAME)) SITE_NAME,
        nvl(b50.location_ci, b51.location_ci) location_ci,
        nvl(b50.location_lac, b51.location_lac) location_lac
    FROM
    (
        select
            msisdn,
            site_name,
            location_ci,
            location_lac
        from
        (
            select
                msisdn,
                site_name,
                location_ci,
                location_lac,
                row_number() over(partition by msisdn order by nbre_apparition_msisdn_site desc) line_number
            from
            (
                SELECT
                    MSISDN,
                    SITE_NAME,
                    location_ci,
                    location_lac,
                    count(*) nbre_apparition_msisdn_site
                FROM MON.SPARK_FT_CLIENT_LAST_SITE_DAY 
                WHERE EVENT_DATE = '###SLICE_VALUE###'
                GROUP BY MSISDN, SITE_NAME, location_ci, location_lac
            ) b500
        ) x
        where line_number = 1
    ) b50
    FULL JOIN
    (
        select
            msisdn,
            site_name,
            location_ci,
            location_lac
        from
        (
            select
                msisdn,
                site_name,
                location_ci,
                location_lac,
                row_number() over(partition by msisdn order by nbre_apparition_msisdn_site desc) line_number
            from
            (
                SELECT
                    MSISDN,
                    SITE_NAME,
                    location_ci,
                    location_lac,
                    count(*) nbre_apparition_msisdn_site
                FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_DAY
                WHERE EVENT_DATE = '###SLICE_VALUE###'
                GROUP BY MSISDN, SITE_NAME , location_ci,location_lac
            ) b510
        ) x
        where line_number = 1
    ) b51
    ON b50.MSISDN = b51.MSISDN
) b5 on b0.SERVED_MSISDN = b5.msisdn
LEFT JOIN (
    select max(site_name) site_name,max(townname) townname,max(region) region,ci,lac from dim.dt_gsm_cell_code group by ci,lac
)b96 on upper(b96.ci) = upper(b5.location_ci) and upper(b96.lac) = upper(b5.location_lac)
    