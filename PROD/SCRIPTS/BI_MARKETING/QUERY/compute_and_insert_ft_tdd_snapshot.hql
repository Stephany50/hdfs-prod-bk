INSERT INTO MON.SPARK_FT_TDD_SNAPSHOT
    SELECT
        A.msisdn,
        A.imei,
        A.activation_date_sim,
        A.profile_infinity,
        CASE WHEN A.date_app_imei > C.date_app_imei THEN C.date_app_imei
             ELSE A.date_app_imei
             END AS date_app_imei,
        nvl(B.trafic_data_mo,0) AS trafic_data_mo,
        CASE WHEN NVL(B.trafic_data_mo, 0) > 0 THEN 1
             ELSE 0
             END AS is_active,
        current_timestamp AS insert_date,
        A.event_date
    FROM
    (
        SELECT DISTINCT
            A2.imei,
            GET_NNP_MSISDN_9DIGITS(A1.access_key) AS msisdn,
            A1.event_date,
            A1.activation_date AS activation_date_sim,
            date_app_imei,
            REPLACE(profile, " ", "|") AS profile_infinity
        FROM 
        (
            SELECT
                UPPER(osp_status) AS osp_status,
                GET_NNP_MSISDN_9DIGITS(access_key) AS access_key,
                MIN(profile) AS profile,
                activation_date,
                '###SLICE_VALUE###' AS event_date
            FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
            WHERE event_date <= DATE_ADD('###SLICE_VALUE###', 1)
            AND event_date >= DATE_SUB('###SLICE_VALUE###', 28)
            AND event_date >= '2022-10-20'
            AND UPPER(TRIM(profile)) LIKE '%INFINITY%'
            AND UPPER(osp_status) = 'ACTIVE'
            GROUP BY
                GET_NNP_MSISDN_9DIGITS(access_key),
                '###SLICE_VALUE###',
                activation_date,
                UPPER(osp_status)
        ) A1
        LEFT JOIN 
        (
            SELECT DISTINCT
                SUBSTR(imei, 1, 14) AS imei,
                GET_NNP_MSISDN_9DIGITS(msisdn) AS msisdn,
                sdate,
                MIN(sdate) OVER (PARTITION BY imei) AS date_app_imei
            FROM MON.SPARK_FT_IMEI_ONLINE
            WHERE sdate <= '###SLICE_VALUE###'
            AND sdate >= DATE_SUB('###SLICE_VALUE###', 29)
            AND sdate >= '2022-10-20'
            AND TRIM(imei) RLIKE '^\\d{14,16}$'
            AND SUBSTR(imei, 1, 8) IN (
                SELECT DISTINCT tac_code
                FROM DIM.TDD_TAC_CODE
            )
        ) A2 
        ON GET_NNP_MSISDN_9DIGITS(A1.access_key) = GET_NNP_MSISDN_9DIGITS(A2.msisdn)
        AND A1.event_date = A2.sdate
    ) A 
    LEFT JOIN 
    (
        SELECT DISTINCT
            session_date,
            CHARGED_PARTY_MSISDN AS msisdn,
            LEFT(served_party_imei, 14) AS imei,
            (NVL(SUM(bytes_received), 0) + NVL(SUM(bytes_sent), 0))/(1024 * 1024) AS trafic_data_mo
        FROM MON.SPARK_FT_CRA_GPRS
        WHERE session_date = '###SLICE_VALUE###'
        AND SUBSTR(served_party_imei, 1, 8) IN 
            (
                SELECT DISTINCT tac_code
                FROM DIM.TDD_TAC_CODE
            )
        GROUP BY 
            LEFT(served_party_imei, 14),
            CHARGED_PARTY_MSISDN,
            session_date
        UNION ALL 
        SELECT DISTINCT
            session_date,
            CHARGED_PARTY_MSISDN AS msisdn,
            LEFT(served_party_imei, 14) AS imei,
            (NVL(SUM(bytes_received), 0) + NVL(SUM(bytes_sent), 0))/(1024 * 1024) AS trafic_data_mo
        FROM MON.SPARK_FT_CRA_GPRS_POST
        WHERE session_date = '###SLICE_VALUE###' 
        AND SUBSTR(served_party_imei, 1, 8) IN (
            SELECT DISTINCT tac_code
            FROM DIM.TDD_TAC_CODE
            )
        GROUP BY 
            LEFT(served_party_imei, 14),
            CHARGED_PARTY_MSISDN,
            session_date
    ) B
    ON A.msisdn = B.msisdn
    AND A.event_date = B.session_date
    AND A.imei = B.imei
    LEFT JOIN 
    (
        SELECT DISTINCT
            imei,
            date_app_imei
        FROM MON.SPARK_FT_TDD_SNAPSHOT
        WHERE event_date = DATE_SUB('###SLICE_VALUE###', 1)
        GROUP BY imei, date_app_imei
    ) C 
    ON A.imei = C.imei