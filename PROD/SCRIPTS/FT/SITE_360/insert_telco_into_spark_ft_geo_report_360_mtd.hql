INSERT INTO MON.SPARK_FT_GEOMARKETING_REPORT_360_MTD
SELECT
    SITE_NAME,
    TOWN,
    REGION_ADM,
    REGION_COMMERCIAL,
    KPI_NAME,
    KPI_VALUE,
    CURRENT_TIMESTAMP() INSERT_DATE,
    '###SLICE_VALUE###' EVENT_DATE
FROM
(
    
    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'CALL_BOX' KPI_NAME,
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
        WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' AND CATEGORY_DOMAIN='NEW_DOMAIN'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
        WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' AND CATEGORY_DOMAIN='NEW_DOMAIN'
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
        
) T