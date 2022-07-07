SELECT
	from_unixtime(unix_timestamp(original_file_date), 'dd/MM/yyyy') original_file_date,
    user_id ,
	user_name,       
	category_code,   
	msisdn ,
	last_name,       
	user_type ,      
	from_unixtime(unix_timestamp(transaction_on), 'dd/MM/yyyy') transaction_on,
	status,
	error_code
FROM CDR.SPARK_IT_OMNY_PIN_MODIFIED
WHERE original_file_date = "###SLICE_VALUE###"