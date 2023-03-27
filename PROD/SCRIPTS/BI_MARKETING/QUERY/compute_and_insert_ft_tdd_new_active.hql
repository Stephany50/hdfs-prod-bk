INSERT INTO MON.SPARK_FT_TDD_NEW_ACTIVE
    SELECT 
        imei,
        msisdn,
        MIN(profile_infinity) AS profile_infinity,
        activation_date_sim,
        date_app_imei,
        current_timestamp insert_date,
        '###SLICE_VALUE###' AS event_date
    FROM MON.SPARK_FT_TDD_SNAPSHOT
    WHERE date_app_imei IS NOT NULL 
    AND date_app_imei = '###SLICE_VALUE###'
    GROUP BY 
        imei,
        msisdn,
        date_app_imei,
        activation_date_sim,
        '###SLICE_VALUE###'