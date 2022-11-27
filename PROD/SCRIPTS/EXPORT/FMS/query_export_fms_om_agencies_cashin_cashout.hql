select
    C.nom_prenom shop,
    -- c.msisdn,
    -- d.account_id,
    transfer_datetime_nq date,
    service_type type,
    transaction_amount amount,
    'Unknown' channel,
    concat(D.user_name, ' ', D.last_name) staff_name,
    transfer_id transaction_id
from (
    select 
        sender_msisdn,
        receiver_msisdn,
        transfer_datetime_nq,
        service_type,
        transaction_amount,
        transfer_id
    from cdr.spark_it_omny_transactions
    where TRANSFER_DATETIME = '###SLICE_VALUE###'
) A
JOIN (
    select
        account_id,
        user_name,
        last_name,
        user_domain
    from cdr.spark_it_om_all_balance
    where original_file_date = '###SLICE_VALUE###' and upper(trim(user_domain)) like '%ORANGE AGENC%'
) D on (D.account_id = A.sender_msisdn or D.account_id = A.receiver_msisdn)
LEFT JOIN (
    select
        msisdn, concat(user_name, ' ', last_name) nom_prenom
    from cdr.spark_it_omny_account_snapshot_new where original_file_date = '###SLICE_VALUE###'
) C on (C.MSISDN = D.account_id)
