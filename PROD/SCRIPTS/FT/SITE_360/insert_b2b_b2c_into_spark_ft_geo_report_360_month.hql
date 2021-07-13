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
            WHEN C.SEGMENTATION = 'B2B' THEN 'B2B'
            WHEN C.SEGMENTATION = 'B2C' THEN 'B2C'
            ELSE NULL
        END KPI_NAME,
        SUM
        (
            case 
                when C.SEGMENTATION in ('B2C', 'B2B') then NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)
                else 0
            end
        ) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION,
            COMMERCIAL_OFFER
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE='OUI'
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    LEFT JOIN DIM.DT_OFFER_PROFILES C ON UPPER(A.COMMERCIAL_OFFER) = UPPER(C.PROFILE_CODE)
    WHERE UPPER(C.SEGMENTATION) IN ('B2B', 'B2C')
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION,
        CASE
            WHEN C.SEGMENTATION = 'B2B' THEN 'B2B'
            WHEN C.SEGMENTATION = 'B2C' THEN 'B2C'
            ELSE NULL
        END
    
) T