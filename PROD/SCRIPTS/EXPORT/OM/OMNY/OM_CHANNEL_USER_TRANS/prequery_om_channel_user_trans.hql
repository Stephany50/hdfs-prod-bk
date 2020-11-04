SELECT IF(
T_1.CDR_COUNT > 0 AND
T_2.CDR_COUNT > 0 AND
T_3.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) CDR_COUNT FROM CDR.SPARK_IT_OMNY_TRANSACTIONS WHERE TRANSFER_DATETIME='###SLICE_VALUE###') T_1,
(SELECT COUNT(*) CDR_COUNT FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE=(SELECT MAX(ORIGINAL_FILE_DATE) FROM CDR.SPARK_IT_OM_ALL_BALANCE WHERE ORIGINAL_FILE_DATE BETWEEN  DATE_SUB('###SLICE_VALUE###', 7) AND '###SLICE_VALUE###')) T_2,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_OM_CHANNEL_USER_TRANS' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_3