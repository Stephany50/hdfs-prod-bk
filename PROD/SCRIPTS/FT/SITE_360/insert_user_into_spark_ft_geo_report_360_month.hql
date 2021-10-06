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
        'DATAUSERS' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI' AND trafic_data>= 1
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI' AND trafic_data>= 1
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    union all

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'VOICEUSERS' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI' AND trafic_voix>0
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI' AND trafic_voix>0
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
    union all


    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'SMSUSERS' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI' AND trafic_sms>0
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI' AND trafic_sms>0
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
) T