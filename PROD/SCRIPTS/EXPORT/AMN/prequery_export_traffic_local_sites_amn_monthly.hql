SELECT IF( T_1.nb_dates = abs(datediff(last_day(concat('###SLICE_VALUE###', '-01')), concat('###SLICE_VALUE###', '-01'))) + 1 
AND nb_lines = nb_ci
AND T_2.NB_EXPORT < 1 ,"OK","NOK")
FROM
(SELECT COUNT(distinct event_date) nb_dates FROM mon.spark_ft_amn_local_traffic_day2 WHERE EVENT_DATE BETWEEN concat('###SLICE_VALUE###', '-01') and last_day(concat('###SLICE_VALUE###', '-01'))) T_1,
(select count(ci) nb_lines, count(distinct ci) nb_ci from dim.dt_ci_lac_site_amn) T3,
(select count(*) NB_EXPORT from (select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='${hivevar:job_instanceid}' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M) T_2