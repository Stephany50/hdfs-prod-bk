SELECT 
 replace(nvl(account_type,''),';','-') account_type,
 replace(nvl(account_name,''),';','-') account_name,
 replace(nvl(account_id,''),';','-') account_id,
 nvl(balance,'') balance,
 replace(nvl(user_name,''),';','-') user_name,
 replace(nvl(last_name,''),';','-') last_name,
 replace(nvl(user_domain,''),';','-') user_domain,
 replace(nvl(user_category,''),';','-') user_category,
 replace(nvl(wallet_number,''),';','-') wallet_number,
 replace(nvl(frozen_amount,''),';','-') frozen_amount,
 replace(nvl(original_file_name,''),';','-') original_file_name,
 nvl(to_date(insert_date),'') insert_date,
 nvl(to_date(original_file_date),'') original_file_date
FROM CDR.SPARK_IT_OM_ALL_BALANCE
WHERE original_file_date ='###SLICE_VALUE###'