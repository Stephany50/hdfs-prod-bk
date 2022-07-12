SELECT
	from_unixtime(unix_timestamp(original_file_date), 'dd/MM/yyyy')  original_file_date ,
	target_user_id  ,
	target_user_msisdn,
	target_user_category_code,
	target_user_name ,
	target_user_last_name,
	changed_by_user_id ,
	changed_by_user_msisdn,
	changed_by_user_category_code,   
	changed_by_user_login_id ,  
	changed_by_user_name ,
	changed_by_user_last_name, 
	from_unixtime(unix_timestamp(created_on), 'dd/MM/yyyy')  created_on, 
	action_type,
	remarks 
FROM CDR.SPARK_IT_OMNY_PIN_RESET
WHERE original_file_date = "###SLICE_VALUE###"