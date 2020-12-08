SELECT IF(T_1.FT_EXIST > 0 AND
T_2.NB_EXPORT < 1 ,"OK","NOK")
FROM
(SELECT COUNT(*) FT_EXIST FROM mon.spark_ft_subscription WHERE transaction_date ='###SLICE_VALUE###') T_1,
(select count(*) NB_EXPORT from
(select * from (SELECT * FROM MON.EXPORT_HISTORY where event_date='###SLICE_VALUE###' and JOB_INSTANCEID='LOAD_FT_A_SUBSCRIPTION_B2B' ORDER BY INSERT_DATE DESC LIMIT 1)  T where T.STATUS = 'OK') M
) T_2
