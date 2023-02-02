INSERT INTO MON.SPARK_FT_TDD_PARC
    SELECT 
        imei,
        msisdn,
        date_app_imei,
        MIN(profile_infinity) AS profile_infinity,
        "ACTIF_30J" AS kpi_name,
        current_timestamp insert_date,
        event_date
    FROM MON.SPARK_FT_TDD_SNAPSHOT
    WHERE is_active = 1
    AND event_date >= DATE_SUB('###SLICE_VALUE###', '29')
    AND event_date <= '###SLICE_VALUE###'
    GROUP BY
        imei,
        msisdn,
        date_app_imei,
        "ACTIF_30J",
        event_date