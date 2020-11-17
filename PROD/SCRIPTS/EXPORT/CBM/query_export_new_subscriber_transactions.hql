SELECT 
    msisdn,
    user_id,
    creation_date,
    user_type,
    modified_on,
    first_transaction_on,
    transfer_id,
    service_type,
    last_transaction,
    transfer_status,
    transaction_amount,
    service_charge_received,
    insert_date,
    event_date
FROM MON.SPARK_FT_OM_NEW_SUBSCRIBER_TRANSACTIONS
WHERE EVENT_DATE = "###SLICE_VALUE###"