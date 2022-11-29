select
    C.nom_prenom shop,
    transfer_datetime_nq date,
    service_type type,
    transaction_amount amount,
    'Unknown' channel,
    concat(D.user_name, ' ', D.last_name) staff_name,
    transfer_id transaction_id
from tmp.fms_om_cashin_cashout_transactions A
JOIN tmp.fms_om_cashin_cashout_agencies D on (D.account_id = A.sender_msisdn or D.account_id = A.receiver_msisdn)
LEFT JOIN tmp.fms_om_cashin_cashout_accounts C on (C.MSISDN = D.account_id)
