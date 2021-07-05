INSERT INTO MON.SPARK_FT_GEOMARKETING_REPORT_360
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
        'TRAFIC_VOIX' KPI_NAME,
        SUM(TRAFIC_VOIX) KPI_VALUE
    FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
    WHERE EVENT_DATE='###SLICE_VALUE###'
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'TRAFIC_DATA' KPI_NAME,
        SUM(TRAFIC_DATA) VALUE
    FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
    WHERE EVENT_DATE='###SLICE_VALUE###'
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
    UNION ALL

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'TRAFIC_SMS' KPI_NAME,
        SUM(TRAFIC_SMS) VALUE
    FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
    WHERE EVENT_DATE='###SLICE_VALUE###'
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'REVENU_VOIX_PYG' KPI_NAME,
        SUM(REVENU_VOIX_PYG) VALUE
    FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
    WHERE EVENT_DATE='###SLICE_VALUE###'
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'REVENU_SMS_PYG' KPI_NAME,
        SUM(REVENU_SMS_PYG) VALUE
    FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR 
    WHERE EVENT_DATE='###SLICE_VALUE###'
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL

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
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
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
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI'
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

    UNION ALL    

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
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI' AND trafic_data>= 1
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI' AND trafic_data>= 1
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
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI' AND trafic_voix>0
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI' AND trafic_voix>0
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
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI' AND trafic_sms>0
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_USAGE_TRAFFIC_REVENU_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI' AND trafic_sms>0
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'DECONNEXION' KPI_NAME, 
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'     
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND DEACTIVATION_DATE = '###SLICE_VALUE###'     
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
        'GROSS_ADD' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
     ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND ACTIVATION_DATE = '###SLICE_VALUE###'     
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND ACTIVATION_DATE = '###SLICE_VALUE###'     
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
        'FAMOCO' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            IDENTIFICATEUR,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND ACTIVATION_DATE = '###SLICE_VALUE###' 
    ) A
    LEFT JOIN
    (
        SELECT
            IDENTIFICATEUR,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND ACTIVATION_DATE = '###SLICE_VALUE###' 
        GROUP BY IDENTIFICATEUR
    ) B ON A.IDENTIFICATEUR = B.IDENTIFICATEUR
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
        'PARC_ART' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_ART = 'OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_ART = 'OUI'
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
        'PARC_GROUPE' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE = 'OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE = 'OUI'
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
    UNION ALL 

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'CALL_BOX' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND CATEGORY_DOMAIN='NEW_DOMAIN'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND CATEGORY_DOMAIN='NEW_DOMAIN'
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL 

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'RECHARGES' KPI_NAME,
        SUM(REFILL_AMOUNT) KPI_VALUE
    FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
    WHERE EVENT_DATE = '###SLICE_VALUE###'
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL 

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        CASE
            WHEN SERVICE_TYPE = 'CASHIN' THEN 'CASHIN'
            WHEN SERVICE_TYPE IN ('CASHOUT', 'COUTBYCODE') THEN 'CASHOUT'
            ELSE NULL
        END KPI_NAME,
        SUM(NVL(TRANSACTION_AMOUNT, 0)) KPI_VALUE
    FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR
    WHERE EVENT_DATE = '###SLICE_VALUE###' AND SERVICE_TYPE IN ('CASHIN', 'CASHOUT', 'COUTBYCODE')
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION,
        CASE
            WHEN SERVICE_TYPE = 'CASHIN' THEN 'CASHIN'
            WHEN SERVICE_TYPE IN ('CASHOUT', 'COUTBYCODE') THEN 'CASHOUT'
            ELSE NULL
        END

    UNION ALL

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'REVENU_OM' KPI_NAME,
        SUM(REVENU_OM) KPI_VALUE
    FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
    WHERE EVENT_DATE = '###SLICE_VALUE###' 
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION

    UNION ALL

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
        WHERE EVENT_DATE = '###SLICE_VALUE###' 

        union 

        SELECT
            receiver_msisdn MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' 
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
            WHERE EVENT_DATE = '###SLICE_VALUE###' 

            union 

            SELECT
                receiver_msisdn MSISDN,
                SITE_NAME,
                TOWN,
                REGION,
                COMMERCIAL_REGION
            FROM MON.SPARK_FT_DATAMART_MARKETING_OM_HOUR
            WHERE EVENT_DATE = '###SLICE_VALUE###' 
        ) T
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
    UNION ALL 

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
        ROUND
        (
            SUM
            (
                case 
                    when C.SEGMENTATION in ('B2C', 'B2B') then NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)
                    else 0
                end
            )
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
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE = '###SLICE_VALUE###' AND EST_PARC_GROUPE='OUI'
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

    UNION ALL 

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'POS_OM' KPI_NAME,
        ROUND(SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0))) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' 
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_DISTRIBUTION_OM_HOUR
        WHERE EVENT_DATE = '###SLICE_VALUE###' 
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
) T