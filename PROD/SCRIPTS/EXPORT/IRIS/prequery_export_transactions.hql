SELECT IF(
T_1.it_omny_transactions > 0 AND
T_3.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) it_omny_transactions FROM cdr.spark_it_omny_transactions WHERE transfer_datetime = '###SLICE_VALUE###' ) T_1,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_EXPORT_TRANSACTIONS' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_3

