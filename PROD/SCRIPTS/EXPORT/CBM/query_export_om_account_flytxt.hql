select 
    msisdn,
    is_active status, 
    user_category, 
    balance, 
    original_file_date 
from cdr.spark_it_omny_account_snapshot_new 
where original_file_date = '###SLICE_VALUE###'
