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
    SELECT 
        'SMS' call_type,
        other_party caller_msisdn,
        served_msisdn called_msisdn,
        served_imsi called_imsi,
        served_imei called_imei,
        'Outgoing' call_direction,
        transaction_date as call_date,
        concat(left(transaction_time, 2), ':', substr(transaction_time, 3, 2), ':', right(transaction_time, 2)) call_time,
        served_party_location msc_id,
        right(served_party_location, 5) cell_id
    FROM MON.SPARK_FT_MSC_TRANSACTION
    WHERE length(served_msisdn) = 9
    AND TRANSACTION_DATE = '###SLICE_VALUE###'
    AND UPPER(TRANSACTION_TYPE) LIKE "%SMS%"
) A
INNER JOIN
(
    SELECT msisdn
    FROM CDR.SPARK_IT_OMNY_ACCOUNT_SNAPSHOT_NEW
    WHERE original_file_date = '###SLICE_VALUE###'
) B
ON A.caller_msisdn = B.msisdn