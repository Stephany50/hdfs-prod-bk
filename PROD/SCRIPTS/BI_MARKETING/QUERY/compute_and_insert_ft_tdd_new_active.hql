INSERT INTO MON.SPARK_FT_TDD_NEW_ACTIVE
    SELECT
        imei,
        msisdn,
        profile_infinity,
        activation_date_sim,
        date_app_imei,
        current_timestamp AS insert_date,
        '###SLICE_VALUE###' AS event_date
    FROM 
    (
        SELECT DISTINCT
            imei,
            msisdn,
            MIN(profile_infinity) AS profile_infinity,
            activation_date_sim,
            MIN(first_date_app_imei) AS date_app_imei
        FROM MON.SPARK_FT_TDD_SNAPSHOT_FINAL
        WHERE first_date_app_imei IS NOT NULL 
        AND first_date_app_imei = '###SLICE_VALUE###'
        GROUP BY 
            imei,
            msisdn,
            activation_date_sim
    )