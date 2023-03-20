INSERT INTO MON.SPARK_FT_TDD_TRAFFIC
    SELECT
        session_date,
        session_hour,
        session_day,
        imei,
        LOCATION_CI,
        location_lac,
        TRAFIC_DATA,
        CURRENT_TIMESTAMP AS insert_date,
        '###SLICE_VALUE###' AS event_date
    FROM
    (
        SELECT
            session_date,
            LEFT(session_time,2) AS session_hour,
            CASE 
                WHEN LEFT(session_time, 2) BETWEEN '00' AND '05' THEN 'fin_nuit'
                WHEN LEFT(session_time, 2) BETWEEN '06' AND '11' THEN 'matinee'
                WHEN LEFT(session_time, 2) BETWEEN '12' AND '17' THEN 'aprem'
                WHEN LEFT(session_time, 2) BETWEEN '18' AND '23' THEN 'debut_nuit'
                ELSE 'NULL'
            END session_day,
            served_party_imei AS imei,
            CASE 
                WHEN LENGTH(gpp_user_location_info) = 16 THEN LPAD(CONV(SUBSTRING(gpp_user_location_info, -4, 4), 16, 10), 5, 0)
                WHEN LENGTH(gpp_user_location_info) = 26 THEN CONV(SUBSTRING(gpp_user_location_info, -7, 7), 16, 10)
                ELSE NULL
            END LOCATION_CI,
            location_lac,
            SUM(NVL(BYTES_RECEIVED,0) + NVL(BYTES_SENT,0))/1024/1024 AS TRAFIC_DATA
        FROM MON.SPARK_FT_CRA_GPRS
        WHERE session_date = '###SLICE_VALUE###'
        AND SUBSTRING(served_party_imei, 1, 8) IN (SELECT DISTINCT tac_code FROM DIM.TDD_TAC_CODE)
        GROUP BY
            session_date,
            LEFT(session_time,2),
            CASE 
                WHEN LEFT(session_time,2) BETWEEN '00' AND '05' THEN 'fin_nuit'
                WHEN LEFT(session_time,2) BETWEEN '06' AND '11' THEN 'matinee'
                WHEN LEFT(session_time,2) BETWEEN '12' AND '17' THEN 'aprem'
                WHEN LEFT(session_time,2) BETWEEN '18' AND '23' THEN 'debut_nuit'
                ELSE 'NULL'
            END,
            served_party_imei,
            CASE 
                WHEN LENGTH(gpp_user_location_info) = 16 THEN LPAD(CONV(SUBSTRING(gpp_user_location_info, -4, 4), 16, 10), 5, 0)
                WHEN LENGTH(gpp_user_location_info) = 26 THEN CONV(SUBSTRING(gpp_user_location_info, -7, 7), 16, 10)
                ELSE NULL
            END, 
            location_lac

        UNION 

        SELECT
            session_date,
            LEFT(session_time,2)session_hour,
            CASE 
                WHEN LEFT(session_time,2) BETWEEN '00' AND '05' THEN 'fin_nuit'
                WHEN LEFT(session_time,2) BETWEEN '06' AND '11' THEN 'matinee'
                WHEN LEFT(session_time,2) BETWEEN '12' AND '17' THEN 'aprem'
                WHEN LEFT(session_time,2) BETWEEN '18' AND '23' THEN 'debut_nuit'
                ELSE 'NULL'
            END session_day,
            served_party_imei  imei,
            (
                CASE 
                    WHEN LENGTH(gpp_user_location_info) = 16 THEN LPAD(CONV(SUBSTRING(gpp_user_location_info, -4, 4), 16, 10), 5, 0)
                    WHEN LENGTH(gpp_user_location_info) = 26 THEN CONV(SUBSTRING(gpp_user_location_info, -7, 7), 16, 10)
                ELSE NULL
                END
            ) LOCATION_CI,
            location_lac,
            SUM(NVL(BYTES_RECEIVED,0) + NVL(BYTES_SENT,0))/1024/1024 TRAFIC_DATA
        FROM MON.SPARK_FT_CRA_GPRS_POST 
        WHERE SESSION_DATE = '###SLICE_VALUE###'
        AND SUBSTR(served_party_imei,1,8) IN (SELECT DISTINCT tac_code FROM DIM.TDD_TAC_CODE) 
        GROUP BY 
                session_date,
                LEFT(session_time,2),
                CASE 
                    WHEN LEFT(session_time,2) BETWEEN '00' AND '05' THEN 'fin_nuit'
                    WHEN LEFT(session_time,2) BETWEEN '06' AND '11' THEN 'matinee'
                    WHEN LEFT(session_time,2) BETWEEN '12' AND '17' THEN 'aprem'
                    WHEN LEFT(session_time,2) BETWEEN '18' AND '23' THEN 'debut_nuit'
                    ELSE 'NULL'
                END,
                served_party_imei,
                (
                    CASE 
                        WHEN LENGTH(gpp_user_location_info) = 16 THEN LPAD(CONV(SUBSTRING(gpp_user_location_info, -4, 4), 16, 10), 5, 0)
                        WHEN LENGTH(gpp_user_location_info) = 26 THEN CONV(SUBSTRING(gpp_user_location_info, -7, 7), 16, 10)
                    ELSE NULL
                    END
                ),
                location_lac
    ) Y