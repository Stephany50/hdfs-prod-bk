SELECT  IF( T_1.nb_job >=3 ,"OK","NOK") SMS_FICHIERS
FROM
(SELECT COUNT(distinct job_source) nb_job FROM MON.missing_files_status WHERE to_date(insert_date) > '###SLICE_VALUE###' ) T_1