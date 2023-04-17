SELECT 
    call_type,
    caller_msisdn,
    called_msisdn,
    called_imsi,
    called_imei,
    call_direction,
    call_date,
    call_time,
    msc_id,
    cell_id
FROM 
(
    SELECT msisdn
    FROM CDR.SPARK_IT_OMNY_ACCOUNT_SNAPSHOT_NEW
    WHERE original_file_date = '###SLICE_VALUE###'
) A
INNER JOIN
(
    SELECT
        'SMS' AS call_type,
        other_party AS caller_msisdn,
        served_msisdn AS called_msisdn,
        served_imsi AS called_imsi,
        served_imei AS called_imei,
        'Outgoing' AS call_direction,
        transaction_date as call_date,
        concat(left(transaction_time, 2), ':', substr(transaction_time, 3, 2), ':', right(transaction_time, 2)) call_time,
        served_party_location AS cell_id,
        right(served_party_location, 5) AS msc_id 
    FROM MON.SPARK_FT_MSC_TRANSACTION
    WHERE TRANSACTION_DATE = '###SLICE_VALUE###'
    AND UPPER(TRANSACTION_TYPE) LIKE "%SMS%"
) B
ON (A.msisdn = B.caller_msisdn OR A.msisdn = B.called_msisdn) 
WHERE LENGTH(caller_msisdn) = 9
AND LENGTH(called_msisdn) = 9