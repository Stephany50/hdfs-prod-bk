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
        'POS_OM' KPI_NAME,
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE 
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR
        WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR
        WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'
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
        'PARC_ACTIF_OM' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM 
    ( 
        
        SELECT
            sender_msisdn MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
        WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###'

        union 

        SELECT
            receiver_msisdn MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
        WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' 
    ) A
    LEFT JOIN
    (
        select
            msisdn,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        from
        (
            SELECT
                sender_msisdn MSISDN,
                SITE_NAME,
                TOWN,
                REGION,
                COMMERCIAL_REGION
            FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
            WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' 

            union 

            SELECT
                receiver_msisdn MSISDN,
                SITE_NAME,
                TOWN,
                REGION,
                COMMERCIAL_REGION
            FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
            WHERE EVENT_DATE between concat(substr('###SLICE_VALUE###', 1, 7), '-01') and '###SLICE_VALUE###' 
        ) T
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
) T