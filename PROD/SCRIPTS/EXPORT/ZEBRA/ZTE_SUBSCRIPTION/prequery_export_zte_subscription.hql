SELECT IF( T_3.NB_EXPORT < 1 ,"OK","NOK")
FROM
(select count(*) NB_EXPORT from (select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M) T_3