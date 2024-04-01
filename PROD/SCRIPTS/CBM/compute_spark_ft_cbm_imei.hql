INSERT INTO MON.SPARK_FT_IMEI_CBM
    SELECT DISTINCT
        A2.imei AS imei,
        A1.msisdn AS msisdn,
        A1.tac_code AS tac_code,
        terminal_type,
        terminal_model,
        terminal_brand,
        '###SLICE_VALUE###' AS transaction_date
    FROM 
    (
        SELECT DISTINCT
            msisdn,
            imei AS tac_code,
            transaction_date,
            terminal_model,
            terminal_type,
            terminal_brand
        FROM MON.SPARK_FT_OTARIE_DATA_TRAFFIC_DAY
        WHERE transaction_date >= DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM-01')
        AND transaction_date <= '###SLICE_VALUE###'
    ) A1
    LEFT JOIN
    (
        SELECT DISTINCT
            msisdn,
            SUBSTRING(imei, 1, 14) AS imei,
            SUBSTRING(imei, 1, 8) AS tac_code,
            sdate
        FROM MON.SPARK_FT_IMEI_ONLINE
        WHERE sdate >= DATE_FORMAT('###SLICE_VALUE###', 'yyyy-MM-01')
        AND sdate <= '###SLICE_VALUE###'
        AND TRIM(imei) RLIKE '^\\d{14,16}$'

    ) A2
    ON GET_NNP_MSISDN_9DIGITS(A1.msisdn) = GET_NNP_MSISDN_9DIGITS(A2.msisdn)
    AND A1.tac_code = A2.tac_code
    WHERE A2.imei IS NOT NULL