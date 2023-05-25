SELECT
        served_party_msisdn AS msisdn, 
        REPLACE(subscription_service_details," ","|") AS ipp,
        SUM(NVL(rated_amount, 0)) AS ca,
        COUNT(subscription_service_details) AS nb_sous,
        transaction_date
    FROM MON.SPARK_FT_SUBSCRIPTION 
    WHERE transaction_date  = '###SLICE_VALUE###'
    AND UPPER(TRIM(subscription_service_details)) LIKE '%INFINITY%'
    AND NVL(rated_amount, 0) > 0
    GROUP BY 
        transaction_date,
        REPLACE(subscription_service_details," ","|"),
        served_party_msisdn
