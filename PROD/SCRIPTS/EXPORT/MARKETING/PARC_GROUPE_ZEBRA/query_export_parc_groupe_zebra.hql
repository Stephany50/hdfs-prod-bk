SELECT
        base_month,
         category,
         user_status,
         (CASE WHEN trim(SITE_NAME) LIKE '%ACROPOLE-IHS' THEN 'ACROPOLE-IHS'
                    WHEN trim(SITE_NAME) = 'NGD-TROUA MALA II' THEN 'NGD-TROUA-MALA-II'
                    WHEN trim(SITE_NAME) = 'SANTA-BARBRA-YDE-IHS' THEN 'SANTA-BARBARA-YDE-IHS'
                    WHEN trim(SITE_NAME) = 'SKY-WAY-HOTEL_IHS' THEN 'SKY-WAY-HOTEL-IHS'
                    WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI¿RE' THEN 'SODIKO-GARE-ROUTIÃ'
                    WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI?RE' THEN 'SODIKO-GARE-ROUTIÃ'
                    WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
                    WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
                    ELSE trim(SITE_NAME) END),
         trim(TOWNNAME),
         admin_region,
         commerc_region,
         sum(total_msisdn)
    FROM MON.SPARK_TF_ZEB_MAST_ZONE_PARC_MONTHLY
    WHERE base_month >=substr('###SLICE_VALUE###',1,7)
    GROUP
        BY base_month,
        category,
        user_status,
        (CASE WHEN trim(SITE_NAME) LIKE '%ACROPOLE-IHS' THEN 'ACROPOLE-IHS'
                    WHEN trim(SITE_NAME) = 'NGD-TROUA MALA II' THEN 'NGD-TROUA-MALA-II'
                    WHEN trim(SITE_NAME) = 'SANTA-BARBRA-YDE-IHS' THEN 'SANTA-BARBARA-YDE-IHS'
                    WHEN trim(SITE_NAME) = 'SKY-WAY-HOTEL_IHS' THEN 'SKY-WAY-HOTEL-IHS'
                    WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI¿RE' THEN 'SODIKO-GARE-ROUTIÃ'
                    WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI?RE' THEN 'SODIKO-GARE-ROUTIÃ'
                    WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
                    WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
                    ELSE trim(SITE_NAME) END),
        trim(TOWNNAME),
        admin_region,
        commerc_region