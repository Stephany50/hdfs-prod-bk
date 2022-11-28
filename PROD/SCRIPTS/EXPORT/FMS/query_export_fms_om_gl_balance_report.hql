select 
    sender_msisdn numero, 
    sender_user_name nom, 
    sender_pre_bal solde_periode_debit, 
    receiver_pre_bal solde_periode_credit, 
    sender_post_bal solde_au_debit, 
    receiver_post_bal solde_au_credit 
from cdr.spark_it_omny_transactions 
where transfer_datetime = '###SLICE_VALUE###'
