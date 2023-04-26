INSERT INTO AGG.SPARK_FT_TDD_TRAFFIC
    SELECT 
        session_date,
        session_hour,
        session_day,
        COUNT(DISTINCT imei) AS nb_imei,
        LOCATION_CI,
        location_lac,
        SUM(TRAFIC_DATA) AS TRAFIC_DATA,
        CURRENT_TIMESTAMP AS insert_date,
        '###SLICE_VALUE###' AS event_date
    FROM MON.SPARK_FT_TDD_TRAFFIC
    WHERE event_date = '###SLICE_VALUE###'
    GROUP BY
        session_date,
        session_hour,
        session_day,
        LOCATION_CI,
        location_lac,
        CURRENT_TIMESTAMP,
        '###SLICE_VALUE###'