INSERT INTO MON.SPARK_FT_GEOMARKETING_REPORT_360_MONTH
SELECT
    SITE_NAME,
    TOWN,
    REGION_ADM,
    REGION_COMMERCIAL,
    KPI_NAME,
    KPI_VALUE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_MONTH
FROM
(
    
    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        CASE 
            WHEN TECHNO_DEVICE = '5G' THEN 'NBRE_DEVICE_5G'
            WHEN TECHNO_DEVICE = '4G' THEN 'NBRE_DEVICE_4G'
            WHEN TECHNO_DEVICE = '3G' THEN 'NBRE_DEVICE_3G'
            WHEN TECHNO_DEVICE in ('2.75G', '2.5G', '2G') THEN 'NBRE_DEVICE_2G'
            ELSE 'NBRE_DEVICE_3G'
        END KPI_NAME,
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            TECHNO_DEVICE,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI'
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION,
        CASE 
            WHEN TECHNO_DEVICE = '5G' THEN 'NBRE_DEVICE_5G'
            WHEN TECHNO_DEVICE = '4G' THEN 'NBRE_DEVICE_4G'
            WHEN TECHNO_DEVICE = '3G' THEN 'NBRE_DEVICE_3G'
            WHEN TECHNO_DEVICE in ('2.75G', '2.5G', '2G') THEN 'NBRE_DEVICE_2G'
            ELSE 'NBRE_DEVICE_3G'
        END
    
) T