SELECT
    'SMS' AS call_type,
    other_party AS caller_msisdn,
    served_msisdn AS called_msisdn,
    served_imsi AS called_imsi,
    served_imei AS called_imei,
    'Outgoing' AS call_direction,
    CONCAT(SUBSTRING('transaction_date', 1, 4), 
           SUBSTRING('transaction_date', 6, 2),
           SUBSTRING('transaction_date', 9, 2)) as call_date,
    transaction_time AS call_time,
    RIGHT(served_party_location, 5) AS msc_id,
    served_party_location AS cell_id
FROM MON.SPARK_FT_MSC_TRANSACTION
WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
AND UPPER(TRANSACTION_TYPE) LIKE "%SMS%"