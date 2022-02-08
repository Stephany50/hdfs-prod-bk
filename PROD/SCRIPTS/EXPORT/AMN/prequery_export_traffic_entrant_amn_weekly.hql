SELECT IF( T_1.nb_dates = abs(datediff(date_sub('###SLICE_VALUE###', 1), date_sub('###SLICE_VALUE###', 7))) + 1 
AND nb_lines = nb_ci AND dayofweek('###SLICE_VALUE###') - 1 = 1
AND T_2.NB_EXPORT < 1 ,"OK","NOK")
FROM
(SELECT COUNT(distinct event_date) nb_dates FROM AGG.SPARK_FT_A_TRAFFIC_ENTRANT_AMN2 WHERE EVENT_DATE between date_sub('###SLICE_VALUE###', 7) and date_sub('###SLICE_VALUE###', 1)) T_1,
(select count(ci) nb_lines, count(distinct ci) nb_ci from dim.dt_ci_lac_site_amn) T3,
(select count(*) NB_EXPORT from (select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M) T_2