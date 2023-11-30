select 
account_type,account_name,account_id as msisdn,balance,user_name,last_name,wallet_number,frozen_amount,payment_type_id,original_file_date 
from cdr.spark_it_om_all_balance 
where account_type='Subscriber' and payment_type_id='12';
