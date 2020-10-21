SELECT IF(
T_1.FT_EXIST > 0 AND
T_2.NB_EXPORT < 1
,"OK","NOK")
FROM
(SELECT COUNT(*) FT_EXIST FROM CDR.SPARK_IT_ZEBRA_MASTER WHERE transaction_date ='###SLICE_VALUE###') T_1,
(
select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_COM_ZEBRA_MASTER' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2
