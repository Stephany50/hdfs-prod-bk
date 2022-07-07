SELECT
	from_unixtime(unix_timestamp(original_file_date), 'dd/MM/yyyy') original_file_date,
	msisdn_parrain  ,
	msisdn_filleul  ,
	type_parrainage ,
	canal_parrainage ,       
	parrain_is_commando,     
	from_unixtime(unix_timestamp(date_parrainage), 'dd/MM/yyyy') date_parrainage 
FROM CDR.SPARK_IT_PARRAINAGE_OMA
WHERE original_file_date = "###SLICE_VALUE###"