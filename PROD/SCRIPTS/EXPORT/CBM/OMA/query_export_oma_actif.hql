SELECT
	from_unixtime(unix_timestamp(original_file_date), 'dd/MM/yyyy') original_file_date,
	msisdn,
	name ,
	from_unixtime(unix_timestamp(date_abonnement), 'dd/MM/yyyy')  date_abonnement 
FROM CDR.SPARK_IT_PARRAINAGE_OMA_ACTIF
WHERE original_file_date = "###SLICE_VALUE###"