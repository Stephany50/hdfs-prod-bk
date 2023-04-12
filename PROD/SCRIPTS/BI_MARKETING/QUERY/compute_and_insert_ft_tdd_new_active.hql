INSERT INTO MON.SPARK_FT_TDD_NEW_ACTIVE
    SELECT
        A.imei,
        A.msisdn,
        A.profile_infinity,
        A.activation_date_sim,
        NVL(B.date_app_imei, A.date_app_imei) AS date_app_imei,
        current_timestamp AS insert_date,
        '###SLICE_VALUE###' AS event_date
    FROM 
    (
        SELECT DISTINCT
            imei,
            msisdn,
            MIN(profile_infinity) AS profile_infinity,
            activation_date_sim,
            MIN(date_app_imei) AS date_app_imei
        FROM MON.SPARK_FT_TDD_SNAPSHOT
        WHERE date_app_imei IS NOT NULL 
        AND date_app_imei = '###SLICE_VALUE###'
        GROUP BY 
            imei,
            msisdn,
            activation_date_sim
    ) A
    LEFT JOIN 
    (
        SELECT DISTINCT
            imei, 
            msisdn,
            date_app_imei,
            event_date
        FROM MON.SPARK_FT_TDD_SNAPSHOT
        WHERE event_date = DATE_SUB('###SLICE_VALUE###', 1)
    ) B 
    ON A.imei = B.imei