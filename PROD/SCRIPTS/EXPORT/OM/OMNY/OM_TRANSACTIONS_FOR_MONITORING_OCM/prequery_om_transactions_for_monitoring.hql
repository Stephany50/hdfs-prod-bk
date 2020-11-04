SELECT IF(
T_1.CDR_COUNT > 0 AND
T_2.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) CDR_COUNT FROM cdr.spark_it_om_subscribers WHERE modification_date='###SLICE_VALUE###') T_1,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_OM_TRANSACTIONS_FOR_MONITORING_OCM' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2