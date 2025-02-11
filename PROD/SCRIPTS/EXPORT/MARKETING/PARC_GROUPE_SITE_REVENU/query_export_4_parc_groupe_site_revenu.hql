SELECT
	BASE_MONTH AS BASE_MONTH,
    ADMINISTRATIVE_REGION AS ADMINISTRATIVE_REGION,
    TOWNNAME AS TOWNNAME,
    SITE_NAME AS SITE_NAME,
    (CASE TOTAL_USERS_BUNDLE_DATA WHEN -1 THEN 0 ELSE TOTAL_USERS_BUNDLE_DATA END) TOTAL_USERS_BUNDLE_DATA,
    MAIN_COST_BUNDLE_DATA AS MAIN_COST_BUNDLE_DATA,
    (CASE TOTAL_USERS_PAYASGO_DATA WHEN -1 THEN 0 ELSE TOTAL_USERS_PAYASGO_DATA END) TOTAL_USERS_PAYASGO_DATA,
    MAIN_COST_PAYASGO_DATA AS MAIN_COST_PAYASGO_DATA,
    TOTAL_USERS_DATA AS TOTAL_USERS_DATA,
    MAIN_COST_DATA AS MAIN_COST_DATA




FROM
    (
    SELECT
        base_month,
        ADMIN_REGION ADMINISTRATIVE_REGION,
        trim(TOWNNAME) TOWNNAME,
        (CASE WHEN trim(SITE_NAME) LIKE '%ACROPOLE-IHS' THEN 'ACROPOLE-IHS'
        WHEN trim(SITE_NAME) = 'NGD-TROUA MALA II' THEN 'NGD-TROUA-MALA-II'
        WHEN trim(SITE_NAME) = 'SANTA-BARBRA-YDE-IHS' THEN 'SANTA-BARBARA-YDE-IHS'
        WHEN trim(SITE_NAME) = 'SKY-WAY-HOTEL_IHS' THEN 'SKY-WAY-HOTEL-IHS'
        WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI¿RE' THEN 'SODIKO-GARE-ROUTIÈE'
        WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI?RE' THEN 'SODIKO-GARE-ROUTIÈE'
        WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
        WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
        ELSE trim(SITE_NAME) END) SITE_NAME,
        count(DISTINCT (CASE WHEN  main_cost_subscr_forfait_data > 0 THEN MSISDN ELSE '' END))-1 total_users_bundle_data ,
        sum(main_cost_subscr_forfait_data) main_cost_bundle_data,
        count(DISTINCT (CASE WHEN main_cost_data > 0 THEN MSISDN ELSE '' END))-1 total_users_payasgo_data,
        sum(main_cost_data) main_cost_payasgo_data,
        count(DISTINCT MSISDN) total_users_data,
        sum(main_cost_subscr_forfait_data + main_cost_data) main_cost_data

    FROM MON.SPARK_TF_BASE_GROUP_CONSO_MONTH
    WHERE base_month = substr('###SLICE_VALUE###',1,7) AND (main_cost_data > 0 OR main_cost_subscr_forfait_data > 0)

    GROUP BY
    base_month,
    ADMIN_REGION ,
    COMMERC_REGION ,
    trim(TOWNNAME) ,
    (CASE WHEN trim(SITE_NAME) LIKE '%ACROPOLE-IHS' THEN 'ACROPOLE-IHS'
    WHEN trim(SITE_NAME) = 'NGD-TROUA MALA II' THEN 'NGD-TROUA-MALA-II'
    WHEN trim(SITE_NAME) = 'SANTA-BARBRA-YDE-IHS' THEN 'SANTA-BARBARA-YDE-IHS'
    WHEN trim(SITE_NAME) = 'SKY-WAY-HOTEL_IHS' THEN 'SKY-WAY-HOTEL-IHS'
    WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI¿RE' THEN 'SODIKO-GARE-ROUTIÈE'
    WHEN trim(SITE_NAME) = 'SODIKO-GARE-ROUTI?RE' THEN 'SODIKO-GARE-ROUTIÈE'
    WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
    WHEN trim(SITE_NAME) = 'TAM-TAM WEEK-END-IHS' THEN 'TAMTAM-WEEKEND-IHS'
    ELSE trim(SITE_NAME) END)
    ) T