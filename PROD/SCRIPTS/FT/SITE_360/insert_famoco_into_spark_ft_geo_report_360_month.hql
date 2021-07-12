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
        'FAMOCO' KPI_NAME,
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
    FROM
    ( 
        SELECT
            IDENTIFICATEUR,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND substr(ACTIVATION_DATE, 1, 7) = '###SLICE_VALUE###' 
    ) A
    LEFT JOIN
    (
        SELECT
            IDENTIFICATEUR,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND substr(ACTIVATION_DATE, 1, 7) = '###SLICE_VALUE###' 
        GROUP BY IDENTIFICATEUR
    ) B ON A.IDENTIFICATEUR = B.IDENTIFICATEUR
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
) T