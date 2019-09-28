SELECT  IF( T_1.nb_job >=3 ,"OK","NOK") SMS_FICHIERS
FROM
(SELECT COUNT(*) SMS_FICHIERS_EXISTS FROM  MON.SMS_FICHIERS WHERE TRANSACTION_DATE='###SLICE_VALUE###') B_1,
(SELECT COUNT(distinct job_source) nb_job FROM MON.missing_files_status WHERE to_date(insert_date) > '###SLICE_VALUE###' ) T_1