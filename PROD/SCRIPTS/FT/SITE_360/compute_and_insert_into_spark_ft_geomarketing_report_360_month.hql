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
        REGION_ADM,
        REGION_COMMERCIAL,
        CASE
            WHEN KPI_NAME = 'TRAFIC_VOIX' THEN 'TRAFIC_VOIX'
            WHEN KPI_NAME = 'TRAFIC_DATA' THEN 'TRAFIC_DATA'
            WHEN KPI_NAME = 'TRAFIC_SMS' THEN 'TRAFIC_SMS'
            WHEN KPI_NAME = 'REVENU_VOIX_PYG' THEN 'REVENU_VOIX_PYG'
            WHEN KPI_NAME = 'REVENU_SMS_PYG' THEN 'REVENU_SMS_PYG'
            WHEN KPI_NAME = 'RECHARGES' THEN 'RECHARGES'
            WHEN KPI_NAME = 'CASHIN' THEN 'CASHIN'
            WHEN KPI_NAME = 'CASHOUT' THEN 'CASHOUT'
            WHEN KPI_NAME = 'REVENU_OM' THEN 'REVENU_OM'
        END KPI_NAME,
        SUM(KPI_VALUE) KPI_VALUE
    FROM MON.SPARK_FT_GEOMARKETING_REPORT_360 
    WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND 
        KPI_NAME IN (
            'TRAFIC_VOIX', 
            'TRAFIC_DATA', 
            'TRAFIC_SMS',
            'REVENU_VOIX_PYG', 
            'REVENU_SMS_PYG',
            'RECHARGES',
            'CASHIN', 
            'CASHOUT',
            'REVENU_OM'
        )
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION_ADM,
        REGION_COMMERCIAL,
        CASE
            WHEN KPI_NAME = 'TRAFIC_VOIX' THEN 'TRAFIC_VOIX'
            WHEN KPI_NAME = 'TRAFIC_DATA' THEN 'TRAFIC_DATA'
            WHEN KPI_NAME = 'TRAFIC_SMS' THEN 'TRAFIC_SMS'
            WHEN KPI_NAME = 'REVENU_VOIX_PYG' THEN 'REVENU_VOIX_PYG'
            WHEN KPI_NAME = 'REVENU_SMS_PYG' THEN 'REVENU_SMS_PYG'
            WHEN KPI_NAME = 'RECHARGES' THEN 'RECHARGES'
            WHEN KPI_NAME = 'CASHIN' THEN 'CASHIN'
            WHEN KPI_NAME = 'CASHOUT' THEN 'CASHOUT'
            WHEN KPI_NAME = 'REVENU_OM' THEN 'REVENU_OM'
        END

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

    UNION ALL    

    -- OK
    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'DATAUSERS' KPI_NAME,
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
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
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
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
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
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

    UNION ALL

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'DECONNEXION' KPI_NAME, 
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND substr(DEACTIVATION_DATE, 1, 7) = '###SLICE_VALUE###' 
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND substr(DEACTIVATION_DATE, 1, 7) = '###SLICE_VALUE###' 
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
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
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
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND substr(ACTIVATION_DATE, 1, 7) = '###SLICE_VALUE###'  
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

    union all 

    SELECT
        SITE_NAME,
        TOWN,
        REGION REGION_ADM,
        COMMERCIAL_REGION REGION_COMMERCIAL,
        'PARC_ART' KPI_NAME,
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_ART = 'OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_ART = 'OUI'
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
        SUM(NVL(1/NBER_TIMES_IN_PARC_GROUPE, 0)) KPI_VALUE
    FROM
    ( 
        SELECT
            MSISDN,
            SITE_NAME,
            TOWN,
            REGION,
            COMMERCIAL_REGION
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE = 'OUI'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR 
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND EST_PARC_GROUPE = 'OUI'
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
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND CATEGORY_DOMAIN='NEW_DOMAIN'
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_DATAMART_DISTRIB_TELCO_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01')) AND CATEGORY_DOMAIN='NEW_DOMAIN'
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

    UNION ALL 

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
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))
    ) A
    LEFT JOIN
    (
        SELECT
            MSISDN,
            COUNT(*) NBER_TIMES_IN_PARC_GROUPE
        FROM MON.SPARK_FT_CLIENT_SITE_TRAFFIC_HOUR
        WHERE EVENT_DATE between concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))
        GROUP BY MSISDN
    ) B ON A.MSISDN = B.MSISDN
    GROUP BY 
        SITE_NAME,
        TOWN,
        REGION,
        COMMERCIAL_REGION
    
) T