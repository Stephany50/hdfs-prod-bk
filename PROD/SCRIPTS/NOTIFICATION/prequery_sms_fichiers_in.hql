SELECT  IF(T_1.SMS_FICHIERS_IN_EXISTS = 0 AND T_2.missing_files_EXISTS >=1 ,"OK","NOK") SMS_FICHIERS_IN_EXISTS
FROM
(SELECT COUNT(*) SMS_FICHIERS_IN_EXISTS FROM  MON.SMS_FICHIERS_IN WHERE TRANSACTION_DATE='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) missing_files_EXISTS FROM mon.missing_files WHERE original_file_date=date_sub('###SLICE_VALUE###',1)) T_2