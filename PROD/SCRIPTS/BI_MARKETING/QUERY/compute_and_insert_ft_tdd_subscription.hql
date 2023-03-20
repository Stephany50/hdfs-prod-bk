INSERT INTO MON.SPARK_FT_TDD_SUBSCRIPTION
    SELECT
        REPLACE(subscription_service_details," ","|") AS ipp,
        SUM(NVL(rated_amount, 0)) AS ca,
        COUNT(subscription_service_details) AS nb_sous,
        current_timestamp insert_date,
        transaction_date
    FROM MON.SPARK_FT_SUBSCRIPTION 
    WHERE transaction_date  = '###SLICE_VALUE###'
    AND UPPER(TRIM(subscription_service_details)) LIKE '%INFINITY%'
    GROUP BY 
        transaction_date,
        REPLACE(subscription_service_details," ","|")