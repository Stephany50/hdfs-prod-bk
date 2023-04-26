INSERT INTO MON.SPARK_FT_TDD_SNAPSHOT_FINAL
    SELECT
            A.msisdn,
            A.imei,
            A.activation_date_sim,
            A.profile_infinity,
            B.first_date_app_imei,
            A.date_app_imei,
            A.trafic_data_mo,
            A.is_active,
            CURRENT_TIMESTAMP AS insert_date,
            A.event_date 
    FROM 
    (
        SELECT 
            msisdn,
            imei,
            activation_date_sim,
            profile_infinity,
            date_app_imei,
            trafic_data_mo,
            is_active,
            event_date
        FROM MON.SPARK_FT_TDD_SNAPSHOT
        WHERE event_date = '###SLICE_VALUE###'
    ) A 
    LEFT JOIN 
    (
        SELECT 
            imei,
            MIN(date_app_imei) AS first_date_app_imei
        FROM MON.SPARK_FT_TDD_SNAPSHOT
        GROUP BY
            imei
    ) B 
    ON A.imei = B.imei