SELECT replace(account_type,';','-') account_type,
 replace(account_name,';','-') account_name,
 replace(account_id,';','-') account_id,
 balance,
 replace(user_name,';','-') user_name,
 replace(last_name,';','-') last_name,
 replace(user_domain,';','-') user_domain,
 replace(user_category,';','-') user_category,
 replace(wallet_number,';','-') wallet_number,
 replace(frozen_amount,';','-') frozen_amount,
 replace(original_file_name,';','-') original_file_name,
 insert_date,
 original_file_date
FROM CDR.SPARK_IT_OM_ALL_BALANCE
WHERE original_file_date ='###SLICE_VALUE###'