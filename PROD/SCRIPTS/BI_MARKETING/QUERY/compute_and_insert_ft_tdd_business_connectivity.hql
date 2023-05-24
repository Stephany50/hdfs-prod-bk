INSERT INTO MON.SPARK_FT_TDD_BUSINESS_INFINITY
    SELECT
        activation_date,
        GET_NNP_MSISDN_9DIGITS(access_key) AS access_key,
        MAX(REPLACE(profile," ","|")) AS profile, 
        UPPER(osp_status) AS osp_status,
        CURRENT_TIMESTAMP AS insert_date,
        DATE_SUB(event_date, 1) AS event_date
    FROM MON.SPARK_FT_CONTRACT_SNAPSHOT
    WHERE event_date = DATE_ADD('###SLICE_VALUE###', 1)
    AND UPPER(TRIM(profile)) LIKE '%BUSINESS%INFINITY%' 
    GROUP BY
        activation_date,
        GET_NNP_MSISDN_9DIGITS(access_key),
        UPPER(osp_status),
        DATE_SUB(event_date,1)