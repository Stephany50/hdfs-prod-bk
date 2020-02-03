SELECT  IF(B_1.SMS_FICHIERS_EXISTS = 0 AND T_1.nb_job >=3 ,"OK","NOK") SMS_FICHIERS
FROM
(SELECT COUNT(*) SMS_FICHIERS_EXISTS FROM  MON.SPARK_SMS_FICHIERS_IN WHERE TRANSACTION_DATE='###SLICE_VALUE###') B_1,
(SELECT COUNT(distinct job_source) nb_job FROM MON.spark_missing_files_status WHERE ORIGINAL_FILE_DATE= '###SLICE_VALUE###' ) T_1