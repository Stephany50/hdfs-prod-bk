SELECT IF(
T_1.FT_COUNT > 0 AND
T_2.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) FT_COUNT FROM cdr.spark_it_om_all_balance WHERE original_file_date='###SLICE_VALUE###') T_1,
(select count(*) NB_EXPORT from (select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M) T_2