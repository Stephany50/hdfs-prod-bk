INSERT INTO MON.SPARK_FT_TDD_ACTIVE_SUBSCRIPTION
    SELECT DISTINCT
        last_date_subs,
        served_party_msisdn AS msisdn,
        REPLACE(subscription_service_details," ","|") AS ipp,
        'NULL' AS total_occurence,
        rated_amount/1.1925/1.02 AS ca,
        active_date,
        expire_date,
        CURRENT_TIMESTAMP AS  insert_date,
        '###SLICE_VALUE###' AS event_date
    FROM
    (
        SELECT DISTINCT
            transaction_date,
            served_party_msisdn,
            subscription_service_details,
            rated_amount,
            MAX(transaction_date) OVER (PARTITION BY served_party_msisdn ORDER BY transaction_date DESC ) last_date_subs ,
            active_date ,
            expire_date   
        FROM MON.SPARK_FT_SUBSCRIPTION
        WHERE transaction_date BETWEEN DATE_SUB('###SLICE_VALUE###',45) AND '###SLICE_VALUE###'
        AND UPPER(TRIM(subscription_service_details)) LIKE '%INFINITY%'
        AND rated_amount > 0
    ) A
    WHERE last_date_subs = transaction_date